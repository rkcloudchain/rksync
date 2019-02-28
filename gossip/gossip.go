package gossip

import "github.com/rkcloudchain/rksync/protos"

// Gossip is the interface of the gossip component
type Gossip interface {
	Gossip(msg *protos.GossipMessage)
}
