package server

import (
	"context"

	api "github.com/DpodDani/proglog/api/v1"
	"google.golang.org/grpc"
)

// have service depend on an interface rather than a concrete implementation
// a.k.a dependency inversion
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

// instantiates a gRPC server, creates our service, then registers our service
// with the server
// gives user a server that just needs a listener for it to accept incoming
// connections
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	// gRPC server will listen on network, handle requests, call our server,
	// and respond to client with the result
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	// register our service with the gRPC server
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

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

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		// server-side handler can repeatedly call Recv() to read the
		// client-to-server message stream.
		// Recv() returns (nil, io.EOF) once it has reached the end of the
		// stream
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		// response server-to-client message stream is sent by repeatedly
		// calling the Send() method
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			// server-side handler can send stream of protobuf messages to
			// the client through Send() method
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
