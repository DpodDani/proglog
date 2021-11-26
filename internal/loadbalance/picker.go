package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

// static (compile-time) check to make sure Picker struct implements
// base.PickerBuilder interface
var _ base.PickerBuilder = (*Picker)(nil)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

// uses Builder pattern to receive a map of subconnections with information
// about those subconnections to set up picker (behind the scenes gRPC connects
// to the addresses that our resolver discovered)
//
// the picker will route consume RPCs to follower servers and product RPCs to
// leader server (the Address attribute helps us differentiate between the two)
func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.
			Address.
			Attributes.
			Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers
	return p
}

var _ balancer.Picker = (*Picker)(nil)

// gRPC gives Pick() a balancer.PickInfo containing the RPC's name and context
// to help picker know what subconnection to pick; Pick() returns a
// subconnection to handle the RPC call.
// Note: header metadata can be read from the context
//
// Optionally, a Done() callback can be set on the result, so that gRPC can
// call it when the RPC completes.
// The callback tells you the RPC's error, trailer metadata, and whether there
// were bytes sent and received to and from the server
func (p *Picker) Pick(info balancer.PickInfo) (
	balancer.PickResult, error,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result balancer.PickResult
	// look at the RPC's method name to determine the call is an append or
	// consume call, and thereby if a leader subconnection or follower
	// subconnection should be picked
	if strings.Contains(info.FullMethodName, "Produce") ||
		len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower()
	}

	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}

	return result, nil
}

// balance consumer calls across followers in round-robin algorithm
func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

// register the picker with gRPC
func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}
