package log

import (
	"bytes"
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
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
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
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err := buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	// replicates the record and appends it to the lader's log
	// the "record" includes the api.Record data and the request type
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
