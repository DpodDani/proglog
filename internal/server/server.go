package server

import (
	api "github.com/DpodDani/proglog/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

// provides static (compile-time) check that grpcServer satisfies the LogServer
// interface (re: https://stackoverflow.com/a/13194635)
//
// LogServer is an interface, and an interface value holds pair of data
// in the form: (dynamic type, value)
// re: https://stackoverflow.com/a/30162432
var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	// embed struct with unimplemented RPC methods
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}
