package gossip

import (
	"crypto/x509"

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

	// AddMemberToChan adds memeber to channel
	AddMemberToChan(chainID string, member common.PKIidType) (*protos.ChainState, error)

	// AddFileToChan adds file to channel
	AddFileToChan(chainID string, file common.FileSyncInfo) (*protos.ChainState, error)

	// GetPKIidOfCert returns the PKI-ID of a certificate
	GetPKIidOfCert(nodeID string, cert *x509.Certificate) (common.PKIidType, error)

	// CreateChannel creates a channel
	CreateChannel(chainID string, files []common.FileSyncInfo) (*protos.ChainState, error)

	// CloseChannel closes a channel
	CloseChannel(chainID string)

	// Stop the gossip component
	Stop()
}

// emittedRKSyncMessage encapsulates isgned rksync message to compose
// with routing filter to be used while message is forwarded
type emittedRKSyncMessage struct {
	*protos.SignedRKSyncMessage
	filter func(id common.PKIidType) bool
}
