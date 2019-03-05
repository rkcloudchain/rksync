package gossip

import (
	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/protos"
)

type channelRoutingFilterFactory func(channel.Channel) filter.RoutingFilter

// Gossip is the interface of the gossip component
type Gossip interface {
	// Gossip sends a message to other peers to the network
	Gossip(msg *protos.RKSyncMessage)

	// Peers returns the NetworkMembers considered alive
	Peers() []common.NetworkMember

	// JoinChan makes the Gossip instance join a channel
	JoinChan(chainID string, leader bool)

	// Stop the gossip component
	Stop()
}

// emittedRKSyncMessage encapsulates isgned rksync message to compose
// with routing filter to be used while message is forwarded
type emittedRKSyncMessage struct {
	*protos.SignedRKSyncMessage
	filter func(id common.PKIidType) bool
}
