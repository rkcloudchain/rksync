package channel

import (
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

// Config is a configuration item of the channel
type Config struct {
	ID                          string
	PublishStateInfoInterval    time.Duration
	PullPeerNum                 int
	PullInterval                time.Duration
	RequestStateInfoInterval    time.Duration
	StateInfoCacheSweepInterval time.Duration
}

// Channel defines an object that deals with all channel-related message
type Channel interface {
	// IsMemberInChan checks whether the given member is eligible to be in the channel
	IsMemberInChan(member common.NetworkMember) bool

	// HandleMessage processes a message sent by a remote peer
	HandleMessage(protos.ReceivedMessage)

	// Initialize allocates the ChainState and should be invoked once per channel per creation
	Initialize([]common.PKIidType, []common.FileSyncInfo) (*protos.ChainState, error)

	// AddMember adds member to the channel
	AddMember(common.PKIidType) (*protos.ChainState, error)

	// AddFile adds file to the channel
	AddFile(common.FileSyncInfo) (*protos.ChainState, error)

	// Stop the channel's activity
	Stop()
}

// Adapter enables the gossipChannel to communicate with gossipService
type Adapter interface {
	GetChannelConfig() Config
	Gossip(message *protos.SignedRKSyncMessage)
	Forward(message protos.ReceivedMessage)
	Send(message *protos.SignedRKSyncMessage, peers ...*common.NetworkMember)
	GetMembership() []common.NetworkMember
	Lookup(pkiID common.PKIidType) *common.NetworkMember
	DeMultiplex(interface{})
}

// GenerateMAC returns a byte slice that is derived from the peer's PKI-ID
// and a channel name
func GenerateMAC(pkiID common.PKIidType, channelID string) []byte {
	preImage := append([]byte(pkiID), []byte(channelID)...)
	return util.ComputeSHA256(preImage)
}
