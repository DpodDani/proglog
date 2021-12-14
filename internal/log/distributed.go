package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	api "github.com/DpodDani/proglog/api/v1"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{
		config: config,
	}

	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

// creates for server, where user's records will be stored
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	// this is the log data structure we created near the beginning of the book
	// the one that contains the segments, which contain the index and data
	// files :)
	l.log, err = NewLog(logDir, l.config)
	return err
}

// configures and creates server's Raft instance
func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil
	}

	logConfig := l.config
	logConfig.Segment.InitialOffset = 1 // this setting is required by Raft
	// where Raft stores commands that need to be applied by FSM
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// stable store is key-value store where Raft stores important metadata
	// eg. server's current term, or candidate the server voted for
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	retain := 1 // controls how many snapshots are retained
	// Raft snapshots data to recover and store data efficiently
	// rather than streaming all the data from Raft leader, new servers would
	// restore from snapshot and then get latest changes from leader.
	// therefore want to snapshot frequently to minimise data differences
	// between snapshot and leader
	//
	// snapshot can be persisted on S3 (for example)
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxpool := 5
	timeout := 10 * time.Second
	// create transport that wraps a stream layer (low-level stream abstraction)
	// transport is a "thing" that Raft uses to connect with server's peers
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxpool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID // unique ID for this server
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	// generally you'll bootstrap a server configured with itself as the only
	// voter, wait until it becomes the leader, and then tell the leader to
	// add more servers to the cluster
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID: config.LocalID,
				// use fully qualified domain name rather than transport's
				// local address, so node will properly advertise itself to
				// its cluster and clients
				Address: raft.ServerAddress(l.config.Raft.BindAddr),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// wrapper around Raft's API to apply requests and return their responses
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	// write one byte to buffer which contains request type (uint8)
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	// replicates the record and appends it to the leader's log
	// the "record" includes the request type followed by the api.Record data
	// returns a "future" that can be used to wait on the application
	future := l.raft.Apply(buf.Bytes(), timeout)
	// future.Error() returns an error if something went wrong with replication
	// for example: it took too long for Raft to process the command, or the
	// server shutdown
	if future.Error() != nil {
		return nil, future.Error()
	}

	// future.Response() returns what our FSM's Apply() method returns
	res := future.Response()
	// check if the response is an error, using type assertion, and returns
	// error if one exists
	// for some reason Raft doesn't return result and error values separately
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

// read records from server's log (considered "relaxed consistency" since read
// operations don't go through Raft)
// Strong consistency, implying up-to-date with writes, would require going
// through Raft, but then reads are less efficient and take longer
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// adds server to Raft cluster
// we add every server as a voter, but Raft allows us to add servers as non-
// voters too (using AddNonVoter())
// non-voters are useful if you just want to replicate state to them, and have
// them act as read-only eventually consistent state
//
// each time you add more voter servers, you increase the probability that
// replications and elections take longer, because the leader has more servers
// that it needs to communicate with to reach a majority!
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// fun fact: removing the leader from the Raft cluster will trigger an election
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// blocks until the cluster has elected a leader or times out
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}

// provides static (compile-time) check that our fsm struct satisfies the
// raft.FSM interface
var _ raft.FSM = (*fsm)(nil)

// Raft defers the running of the business logic to the finite-state machine,
// also known as FSM
// the FSM has access to our log (the "data store") and appends records to the
// log
//
// if we were writing a key-value service, FSM would update the store of the
// data, eg. a map, a Postgres database etc.
type fsm struct {
	log *Log // the data our FSM manages is our log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Raft invokes this method after committing a log entry
// *raft.Log comes from Raft's managed log store
func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

// unmarshal request and append record to the local log, then return the
// response for Raft to send back to where we called raft.Apply() in
// DistributedLog's apply() function
func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return nil
	}
	return &api.ProduceResponse{Offset: offset}
}

// Raft periodically calls this method to snapshot its state.
// returns an raft.FSMSnapshot that represents a PiT snapshot of the FSM's
// state --> in our case this state is the FSM's log --> so return io.Reader
// that will read all the log's data!
//
// called as per configured SnapshotInterval (how often Raft checks if it
// should snapshot - default 2 minutes) and SnapshotThreshold (how many logs
// since the last snapshot before making a new snapshot - default is 8192)
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

// static (compile-time) check that our snapshot struct satisfies the
// raft.FSMSnapshot interface
var _ raft.FSMSnapshot = (*snapshot)(nil)

// serves two purposes:
// 1) allow Raft to compact its log so it doesn't store logs whose commands
// 	  Raft has already applied
// 2) allow Raft to bootstrap new servers more efficiently than if leader had
//    to replicate its entire log again and again
type snapshot struct {
	reader io.Reader
}

// Raft calls Persist() on FSMSnapshot to write its state to some sink
// (somewhere to store the bytes in)
// sink could be in-memory, a file, S3 bucket etc. - depends on snapshot store
// configured for Raft
// we configured our Raft to use a file snapshot store, so when the snapshot
// completes, we'll have a file containing all the Raft's log data
//
// a shared state store such as S3 would put the burden of writing/reading
// snapshot on S3, rather than the leader, thereby allowing new servers to
// restore snapshots without streaming from the leader
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Raft calls Release() when it has finished with the snapshot
func (s *snapshot) Release() {}

// Raft calls this to restore a FSM from a snapshot
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// number of bytes to read record data
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		// reset log and configure its initial offset to the 1st record we read
		// from the snapshot so the log's offset match.
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		// then we read records in the snapshot and append them to our new log
		if _, err = f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}
	return nil
}

// static (compile-time) check that logStore satisfies raft.LogStore interface
var _ raft.LogStore = (*logStore)(nil)

// where Raft stores the commands
// an API wrapper around our Log data structure (created in the first few
// chapters) to satisfy the raft.LogStore interface
type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset // what we call offset, Raft calls index
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return nil
		}
	}
	return nil
}

// used to remove records that are old or stored in snapshot
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

// static (compile-time) check that our StreamLayer struct satisfies Raft's
// StreamLayer interface
var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config // used to accept incoming connections
	peerTLSConfig   *tls.Config // used to create outgoing connections
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// makes outgoing connections to other servers in Raft cluster
// when we connect to a server, we write the RaftRPC byte to identify the
// connection type so we can multiplex Raft on the same port as our Log gRPC
// request
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// indicate to mux that this is a raft RPC
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

// a mirror of Dial() - accept the incoming connection and read the byte that
// identifies the connection, then create server-side TLS connection
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// returns listener's address
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
