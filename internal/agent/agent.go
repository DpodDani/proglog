package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"

	"github.com/DpodDani/proglog/internal/auth"
	"github.com/DpodDani/proglog/internal/discovery"
	"github.com/DpodDani/proglog/internal/log"
	"github.com/DpodDani/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// runs on every service instance, setting up and connecting all the different
// components (ie. log, membership, server and replicator)
type Agent struct {
	Config

	mux        cmux.CMux // multiplexer for network connections
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// agent's config is comprised of its components' parameters (in order to pass
// them through to the actual components)
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// returns agent, and runs set of methods that setup components
// we expect to have a running functioning service after running New()
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve() // tell mux to serve connections

	return a, nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		return err
	}
	return nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// creates listener on our RPC address that'll accept both Raft and gRPC
// connections. Then create mux with the listener, which will accept
// connections on that listener, and match connections based on configured
// rules.
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		a.Config.RPCPort,
	)

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	a.mux = cmux.New(ln)
	return nil
}

func (a *Agent) setupLog() error {
	// configure the mux that matches Raft connections
	// Raft connections are identified by reading one byte and checking it
	// matches the RaftRPC byte (declared in distributed.go) that's written
	// to the StreamLayer
	//
	// if mux identifies this rule, it will pass connection to the raftLn
	// Listener for Raft to handle the connection
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})

	logConfig := log.Config{}
	// configure the DistributedLog's Raft to use the multiplexed listener
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)

	if err != nil {
		return err
	}

	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}

	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// we use cmux.Any() because we know all other connections must be gRPC
	// connections (because we have a dedicated mux rule for Raft connections)
	grpcLn := a.mux.Match(cmux.Any())

	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	// membership now tells the DistributedLog when servers join/leave cluster
	// replication is now handled by Raft
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})

	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave, // leave Serf cluster
		func() error {
			a.server.GracefulStop() // stops server from receiving new requests
			return nil
		},
		a.log.Close,
	}

	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}
