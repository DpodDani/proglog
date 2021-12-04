package main

import (
	"log"
	"os"
	"path"

	"github.com/DpodDani/proglog/internal/agent"
	"github.com/DpodDani/proglog/internal/config"
	"github.com/spf13/cobra"
)

func main() {
	cli := &cli{}

	// defines the sole command
	cmd := &cobra.Command{
		Use:     "proglog",
		PreRunE: cli.setupConfig, // hook function
		// contains primary logic
		// calls this function when command runs
		RunE: cli.run,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

// contains logic and data common to all commands
type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	cmd.Flags().String("config-file", "", "Path to config file.")

	dataDir := path.Join(os.TempDir(), "proglog")
	cmd.Flags().String("data-dir",
		dataDir,
		"Directory to store log and Raft data.")

	cmd.Flags().String("node-name", hostname, "Unique server ID.")

	cmd.Flags().String("bind-addr",
		"127.0.0.1:8401",
		"Address to bind Serf on.")

	cmd.Flags().Int("rpc-port",
		8400,
		"Port for RPC clients (and Raft) connections.")

	cmd.Flags().StringSlice("start-join-addrs",
		nil,
		"Serf addresses to join.")

	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")

	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")

	cmd.Flags().String("server-tls-cert-file", "", "Path to server TLS cert.")
	cmd.Flags().String("server-tls-key-file", "", "Pagh to server TLS key.")
	cmd.Flags().String("server-tls-ca-file",
		"",
		"Path to server certificate authority")

	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer TLS cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Pagh to peer TLS key.")
	cmd.Flags().String("peer-tls-ca-file",
		"",
		"Path to peer certificate authority")

	return viper.BindFlags(cmd.Flags())
}
