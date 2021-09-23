package discovery

import (
	"net"

	"github.com/hashicorp/raft"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// tyoe that wraps Serf to provide discovery and cluster membership to our
// services
// Users call New() to create a Membership with required configuration
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// configures a Serf instance and starts eventsHandler() goroutine to handle
// Serf events
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	// listen on the following address and port for gossip
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	// event channel is how you'll receives Serf events when a node wants to
	// join or leave the cluster
	// Calling Members() returns a snapshot of the members at any point in time
	config.EventCh = m.events
	// Serf shares these tags to other nodes in cluster for simple data that
	// informs the cluster how to handle this node
	// E.g. node could share its RPC address with a Serf tag, so other nodes
	// know which address to send their RPCs along
	config.Tags = m.Tags                // obtain it from Config field inside Membership struct
	config.NodeName = m.Config.NodeName // acts as node's unique identifier
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()

	// the StartJoinAddrs field contains a list of addresses of nodes in an
	// existing Serf cluster.
	// This information allows a new node to join this existing cluster.
	// When the node joins the cluster, it learns about all the other nodes on
	// the cluster, and those other nodes learn about this new node.
	// This is Serf's gossip protocol at work :)
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// represents some component in our service that needs to know when a server
// joins/leaves cluster
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// runs in a loop and reads events sent by Serf into the events channel
// when node joins/leaves clusters, Serf sends event to all nodes (including
// the node that joined/left cluster)
//
// we check whether node we got an event for is the local server so that it
// doesn't act on itself (eg. we don't want it to replicate itself)
//
// Serf may coalesce multiple member updates into one event, that's why we
// iterate over the event's members
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// returns PiT snapshot of cluster's Serf members
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// tells member to leave the Serf cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	// Raft will error and return ErrNotLeader when you try to change clusster
	// on non-leader nodes
	// logError() will log non-leader errors at the debug level (remainder of
	// logs will be logged at critical level)
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
