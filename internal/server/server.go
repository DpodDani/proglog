package server

import (
	"context"

	api "github.com/DpodDani/proglog/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"

	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// have service depend on an interface rather than a concrete implementation
// a.k.a dependency inversion
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog   CommitLog
	Authorizer  Authorizer
	GetServerer GetServerer
}

// these constants match the values in our policy.csv file
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

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
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server, error,
) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		// map request durations to zap fields
		// ref: https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/logging/zap/options.go#L83
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",         // zap field name
					duration.Nanoseconds(), // value
				)
			},
		),
	}

	// configure OpenCensus to always sample the traces.
	//
	// in prod you may not want EVERY request because it could affect
	// performance, require too much data, or trace confidential data.
	//
	// probability sampler can be used to sample percentage of requests in this
	// case.
	// Downside: it can result in missing important requests.
	// Resolution: write your own sampler that always traces important requests,
	// samples percentage of other requests.
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	// views specify what stats OpenCensus will collect
	// default views track stats on:
	// 	1. received bytes per RPC
	//	2. sent bytes per RPC
	// 	3. latency
	//	4. completed RPCs
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	// configure gRPC to apply Zac interceptors that log gRPC calls, and
	// attach OpenConsensus as server's stats handler so that OpenCensus can
	// record stats on server's request handling.
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	// gRPC server will listen on network, handle requests, call our server,
	// and respond to client with the result
	gsrv := grpc.NewServer(opts...)
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

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

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
	req *api.ConsumeRequest,
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

func (s *grpcServer) GetServers(
	ctx context.Context, req *api.GetServersRequest,
) (
	*api.GetServersResponse, error,
) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}

// create new interface for the GetServers() function, rather than adding it
// to the CommitLog interface, because not every Log that satisfies the
// CommitLog interface has the GetServers() function (for example our regular
// log structure created early on in the book!)
type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

// an interceptor that reads the subject out of the client's certificate
// and writes it to the RPC's context.
//
// interceptors are used to intercept and modify each RPC call, allowing you
// to break the request handling into smaller, reusable chunks.
//
// Interceptors are also known as middleware in other langauges
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// returns the client's certificate's subject, so we can identify a client
// and check their access
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
