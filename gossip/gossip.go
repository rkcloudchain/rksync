package gossip

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
)

// Gossip is the interface of the gossip component
type Gossip interface {
	// Gossip sends a message to other peers to the network
	Gossip(msg *protos.RKSyncMessage)

	// Peers returns the NetworkMembers considered alive
	Peers() []common.NetworkMember

	// SuspectPeers makes the gossip instance validate identities of suspected peers, and close
	// any connections to peers with identities that are found invalid
	SuspectPeers(s common.PeerSuspector)

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)
}

// emittedRKSyncMessage encapsulates isgned rksync message to compose
// with routing filter to be used while message is forwarded
type emittedRKSyncMessage struct {
	*protos.SignedRKSyncMessage
	filter func(id common.PKIidType) bool
}
