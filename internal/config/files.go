package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile            = configFile("ca.pem")
	ServerCertficiate = configFile("server.pem")
	ServerKeyFile     = configFile("server-key.pem") // server private key
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}
