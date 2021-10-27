package loadbalance

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// resolver is used by gRPC to discover servers
// this struct will call the GetServers() endpoint and pass this information to
// gRPC so that the picker knows what servers it can route requests to
type Resolver struct {
	mu sync.Mutex
	// clientConn --> user's client connection for resolver to update with the
	// servers it discovers
	clientConn resolver.ClientConn
	// resolverConn --> resolver's own client connection to the server, so that
	// it can call GetServers() and get the servers
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// static (compile-time) check that Resolver struct satisfies the
// resolver.Builder interface
var _ resolver.Builder = (*Resolver)(nil)

// receives data need to build a resolver that can discover servers and update
// client about discovered servers
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc

	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}

	// raw string literals delimited by backticks are interpreted literally
	// I guess this is needed because ParseServiceConfig takes in a JSON string
	// ref: https://yourbasic.org/golang/multiline-string/
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)

	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil

	return nil, nil
}

const Name = "proglog"

// returns resolver's scheme identifier
// when you call grpc.Dial, gRPC parses out the scheme from the target address
// and tries to find a resolver that matches (defaulting to its DNS resolver)
//
// our resolver will format the target address as follows:
// --> proglog://your-service-address
func (r *Resolver) Scheme() string {
	return Name
}

// register our resolver with gRPC so that it knows about this resolver when
// it's looking for resolvers that match the target scheme
func init() {
	resolver.Register(&Resolver{})
}
