package log

import (
	"context"
	"sync"

	api "github.com/DpodDani/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// the replicator connects to other servers with a gRPC cilent (that we need to
// configure so that it can authenticate with the servers).
// the replicator calls the produce function to save local copy of the messages
// consumed from other servers
// the servers field is a map of server addresses to a channel, which the
// replicator uses to stop replicating from servers when they've failed or left
// the cluster
type Replicator struct {
	DialOptions []grpc.DialOption // used to configure client
	LocalServer api.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// adds given server address to list of servers to replicate, and kicks off
// goroutine to run the actual replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok { // already replicating, so skip
		return nil
	}

	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])

	return nil
}

// create client, open up a consumption stream and consume all logs on server
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	// TODO: how does this goroutine exit? Does it get an error when function
	// that initialised stream variable exits? Is this goroutine a closure
	// function since it references a variable (stream) from an outer scope?
	// TODO: Look out for the "failed to receive" message in the logs!
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// consume the logs from the server in a stream and then produce to the
	// local server to save a copy
	// do this until the server fails or leaves the cluster, which causes the
	// replicator to close the channel for that server, which breaks the loop
	// and ends the replicate() goroutine call
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce (replicate)", addr)
				return
			}
		}
	}
}

// handles server leaving cluster by removing server from list of servers
// to replicate, and closes server's associated channel
// closing the channel signals to receiver in replicate() goroutine to stop
// replicating from server
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	// ok is false if there are no more values to receive from channel and the
	// channel is closed
	// channel is usually closed by the sender
	// closing a channel is only necessary if receiver must be told that no
	// more values are coming
	// ref: https://tour.golang.org/concurrency/4
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// lazily initialises the server map
// lazy initialisation is useful for giving structs a useful zero value,
// because having useful zero values reduce the API's size and complexity
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// closes the replicator so that it doesn't replicate new servers that join
// the cluster, and it stops replicating existing servers
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// tangent note: a techinque for exposing error messages to users is by
// having an errors channel and sending errors along channel for users to
// receive and handle :)
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
