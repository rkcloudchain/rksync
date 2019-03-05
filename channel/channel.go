package channel

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

// Channel defines an object that deals with all channel-related message
type Channel interface {
	// IsMemberInChan checks whether the given member is eligible to be in the channel
	IsMemberInChan(member common.NetworkMember) bool

	// HandleMessage processes a message sent by a remote peer
	HandleMessage(protos.ReceivedMessage)

	// UpdateMembers updates the member of the channel
	UpdateMembers([]common.PKIidType)

	// UpdateFiles updates the file of the channel
	UpdateFiles([]*common.FileSyncInfo)

	// Stop the channel's activity
	Stop()
}

// Adapter enables the gossipChannel to communicate with gossipService
type Adapter interface {
	GetChannelConfig() config.ChannelConfig
	Gossip(message *protos.SignedRKSyncMessage)
	Forward(message protos.ReceivedMessage)
	Send(message *protos.SignedRKSyncMessage, peers ...*common.NetworkMember)
	GetMembership() []common.NetworkMember
	Lookup(pkiID common.PKIidType) *common.NetworkMember
}

// GenerateMAC returns a byte slice that is derived from the peer's PKI-ID
// and a channel name
func GenerateMAC(pkiID common.PKIidType, channelID string) []byte {
	preImage := append([]byte(pkiID), []byte(channelID)...)
	return util.ComputeSHA256(preImage)
}

// func newChainStateCache(sweepInterval time.Duration, hasExpired func(interface{}) bool) *chainStateCache {
// 	membershipStore := lib.NewMembershipStore()
// 	pol := protos.NewRKSyncMessageComparator()

// 	s := &chainStateCache{
// 		MembershipStore: membershipStore,
// 		stopChan:        make(chan struct{}),
// 	}
// 	invalidationTrigger := func(m interface{}) {
// 		pkiID := m.(*protos.SignedRKSyncMessage).GetState().PkiId
// 		membershipStore.Remove(pkiID)
// 	}
// 	s.MessageStore = lib.NewMessageStore(pol, invalidationTrigger)

// 	go func() {
// 		for {
// 			select {
// 			case <-s.stopChan:
// 				return
// 			case <-time.After(sweepInterval):
// 				s.Purge(hasExpired)
// 			}
// 		}
// 	}()

// 	return s
// }

// // chainStateCache is actually a messageStore
// // that also indexes messages that are added
// // so that they could be extracted later
// type chainStateCache struct {
// 	*lib.MembershipStore
// 	lib.MessageStore
// 	stopChan chan struct{}
// }

// func (cache *chainStateCache) Add(msg *protos.SignedRKSyncMessage) bool {
// 	if !cache.MessageStore.CheckValid(msg) {
// 		return false
// 	}
// 	added := cache.MessageStore.Add(msg)
// 	if added {
// 		pkiID := msg.GetState().PkiId
// 		cache.MembershipStore.Put(pkiID, msg)
// 	}
// 	return added
// }

// func (cache *chainStateCache) Stop() {
// 	cache.stopChan <- struct{}{}
// }
