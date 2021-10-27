package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/DpodDani/proglog/api/v1"
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

	// indicate that we want to use the proglog load balancer (which we will
	// implement a little later).
	//
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

// compile-time check to make sure our Resolver satisfies the resolver.Resolver
// interface
var _ resolver.Resolver = (*Resolver)(nil)

// gRPC calls this function to resolve the target, discover the servers and
// update the client connection with the discovered servers
//
// this function can be called concurrently so we use mutexes to protect
// access across goroutines
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
	}

	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}

	// update the state with slice of addresses to inform load balancer what
	// servers it can choose from.
	//
	// resolver.Address has 3 fields:
	// 	1) Addr --> address of the server to connect to.
	//	2) Attributes --> map containing any data that's useful for load
	//	   balancer. We use it to indicate whether a server is a leader or not.
	//	3) ServerName --> name used as the transport certificate authority
	//	   for the address, instead of the hostname taken from the Dial()
	//	   target string.
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
