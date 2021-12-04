package main

import (
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/DpodDani/proglog/internal/agent"
	"github.com/DpodDani/proglog/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	cli := &cli{}

	// defines the sole command
	cmd := &cobra.Command{
		Use:     "proglog",
		PreRunE: cli.setupConfig, // hook function called before RunE
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

	return viper.BindPFlags(cmd.Flags())
}

// reads configuration and prepares the agent's configuration
func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	// Viper provides centralised config registry system, such that multiple
	// configuration sources (eg. file, CLI flags etc.) can set the
	// configuration BUT everything is handled (read) in one place!
	viper.SetConfigFile(configFile)

	if err = viper.ReadInConfig(); err != nil {
		// it's okay if config file doesn't exist
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	c.cfg.ACLModelFile = viper.GetString("acl-model-file")

	c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")

	c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

	if c.cfg.ServerTLSConfig.CertFile != "" &&
		c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.ServerTLSConfig.Server = true
		c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(
			c.cfg.ServerTLSConfig,
		)
		if err != nil {
			return err
		}
	}

	if c.cfg.PeerTLSConfig.CertFile != "" &&
		c.cfg.PeerTLSConfig.KeyFile != "" {
		c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(
			c.cfg.PeerTLSConfig,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	var err error
	// create agent
	agent, err := agent.New(c.cfg.Config)
	if err != nil {
		return err
	}

	sigc := make(chan os.Signal, 1)
	// handle signals from operating system
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	// shut down agent gracefully when operating system terminates program
	return agent.Shutdown()
}
