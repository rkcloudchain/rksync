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
	// SelfPKIid returns the peer's PKI-ID
	SelfPKIid() common.PKIidType

	// SelfChannelInfo returns the peer's latest ChainState message of a given channel
	SelfChannelInfo(chainID string) *protos.ChainState

	// AddMemberToChan adds memeber to channel
	AddMemberToChan(chainID string, member common.PKIidType) (*protos.ChainState, error)

	// AddFileToChan adds file to channel
	AddFileToChan(chainID string, file common.FileSyncInfo) (*protos.ChainState, error)

	// GetPKIidOfCert returns the PKI-ID of a certificate
	GetPKIidOfCert(nodeID string, cert *x509.Certificate) (common.PKIidType, error)

	// InitializeChannel initialize channel
	InitializeChannel(chainID string, chainState *protos.ChainState) error

	// CreateChannel creates a channel
	CreateChannel(chainID string, files []common.FileSyncInfo) (*protos.ChainState, error)

	// CloseChannel closes a channel
	CloseChannel(chainID string)

	// GetPeers returns the NetworkMembers considered alive
	Peers() []common.NetworkMember

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)

	// Stop the gossip component
	Stop()
}

// emittedRKSyncMessage encapsulates isgned rksync message to compose
// with routing filter to be used while message is forwarded
type emittedRKSyncMessage struct {
	*protos.SignedRKSyncMessage
	filter func(id common.PKIidType) bool
}
