package server

import (
	"io/ioutil"
	"net"
	"testing"

	"github.com/DpodDani/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	// marks this function as a test helper function
	// file and line information will not be printed for this function
	t.Helper()

	// create a server
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// contains connection configuration info.
	// clientOptions --> a slice containing grpc.DialOption objects
	// WithInsecure --> disables transport security for the connection
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	// creates client connection to l.Addr()
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	// accessing function/struct from log package
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// using the Config struct in the server package this time!
	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Server(l)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}
