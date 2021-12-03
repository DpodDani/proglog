package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/DpodDani/proglog/api/v1"

	"github.com/DpodDani/proglog/internal/agent"
	"github.com/DpodDani/proglog/internal/config"
	"github.com/DpodDani/proglog/internal/loadbalance"
	"github.com/stretchr/testify/require"
)

func TestAgent(t *testing.T) {
	// defines configuration of certificate used by each server node
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	// defines configuration of certificate that's served between servers so
	// that they can connect with and replicate each other
	// servers act as "clients" of other servers when it comes to replicating
	// from other servers!
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 3; i++ { // setup a 3-node cluster
		// one port for Serf discovery connections,
		// and another for gRPC log connections
		ports := dynaport.Get(2) // return 2 free ports
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		// 2nd and 3rd nodes join the 1st node in the cluster
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agent, err := agent.New(
			agent.Config{
				ServerTLSConfig: serverTLSConfig,
				PeerTLSConfig:   peerTLSConfig,
				DataDir:         dataDir,
				BindAddr:        bindAddr,
				RPCPort:         rpcPort,
				NodeName:        fmt.Sprintf("%d", i),
				StartJoinAddrs:  startJoinAddrs,
				ACLModelFile:    config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
				Bootstrap:       i == 0,
			},
		)
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	// defer this after the sleep function call at the end
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agent.Config.DataDir),
			)
		}
	}()

	time.Sleep(3 * time.Second) // give nodes time to discover each other

	// produce message to the 1st node in the cluster
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// wait for record to be replicated
	time.Sleep(3 * time.Second)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// wait until replication has finished
	// time.Sleep(3 * time.Second)

	// consume from the 2nd node in the cluster
	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// code below demonstrates how we can consume multiple records from the
	// original server, because it has replicated data from another server that
	// replicated its data from the original server, and the cycle repeats
	// infinitely (until we call agent.Shutdown() in the deferred function)
	//
	// this "infinite" replication will be solved with coordination in the
	// next chapter

	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

// agent argument determines which node (in our 3-node cluster) our log client
// connects to (for reading/writing)
func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(fmt.Sprintf(
		"%s:///%s",
		loadbalance.Name,
		rpcAddr,
	), opts...)

	// conn, err := grpc.Dial(fmt.Sprintf(
	// 	"%s",
	// 	rpcAddr,
	// ), opts...)

	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
