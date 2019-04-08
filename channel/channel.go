/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

// Config is a configuration item of the channel
type Config struct {
	FileSystem                  config.FileSystem
	PublishStateInfoInterval    time.Duration
	PullPeerNum                 int
	PullInterval                time.Duration
	RequestStateInfoInterval    time.Duration
	StateInfoCacheSweepInterval time.Duration
}

// Channel defines an object that deals with all channel-related message
type Channel interface {
	Self() *protos.ChainState

	// IsMemberInChan checks whether the given member is eligible to be in the channel
	IsMemberInChan(member common.NetworkMember) bool

	// HandleMessage processes a message sent by a remote peer
	HandleMessage(protos.ReceivedMessage)

	// Initialize allocates the ChainState and should be invoked once per channel per creation
	Initialize(string, []common.PKIidType, []common.FileSyncInfo) (*protos.ChainState, error)

	// InitializeWithChanState allocates the ChainState message
	InitializeWithChainState(*protos.ChainState) error

	// AddMember adds member to the channel
	AddMember(common.PKIidType) (*protos.ChainState, error)

	// RemoveMember removes member contained in the channel
	RemoveMember(common.PKIidType) (*protos.ChainState, error)

	// AddFile adds file to the channel
	AddFile(common.FileSyncInfo) (*protos.ChainState, error)

	// RemoveFile removes file contained in the channel
	RemoveFile(string) (*protos.ChainState, error)

	// Stop the channel's activity
	Stop()
}

// Adapter enables the gossipChannel to communicate with gossipService
type Adapter interface {
	GetChannelConfig() Config
	Gossip(message *protos.SignedRKSyncMessage)
	Forward(message protos.ReceivedMessage)
	Send(message *protos.SignedRKSyncMessage, peers ...*common.NetworkMember)
	SendWithAck(message *protos.SignedRKSyncMessage, timeout time.Duration, minAck int, peers ...*common.NetworkMember) error
	GetMembership() []common.NetworkMember
	Lookup(pkiID common.PKIidType) *common.NetworkMember
	DeMultiplex(interface{})
	Unregister([]byte)
	Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)
}

// GenerateMAC returns a byte slice that is derived from the peer's PKI-ID
// and a channel name
func GenerateMAC(pkiID common.PKIidType, channelID string) common.ChainMac {
	preImage := append([]byte(pkiID), []byte(channelID)...)
	return util.ComputeSHA3256(preImage)
}

func contains(files []*protos.File, target string) bool {
	for _, file := range files {
		if file.Path == target {
			return true
		}
	}
	return false
}
