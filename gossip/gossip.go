/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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

	// SelfChainInfo returns the peer's latest ChainState message of a given channel
	SelfChainInfo(chainID string) *protos.ChainState

	// AddMemberToChain adds member to channel
	AddMemberToChain(chainMac common.ChainMac, member common.PKIidType) (*protos.ChainState, error)

	// RemoveMemberWithChain removes member contained in the channel
	RemoveMemberWithChain(chainMac common.ChainMac, member common.PKIidType) (*protos.ChainState, error)

	// AddFileToChain adds file to channel
	AddFileToChain(chainMac common.ChainMac, file common.FileSyncInfo) (*protos.ChainState, error)

	// RemoveFileWithChain removes file contained in the channel
	RemoveFileWithChain(chainMac common.ChainMac, filename string) (*protos.ChainState, error)

	// GetPKIidOfCert returns the PKI-ID of a certificate
	GetPKIidOfCert(nodeID string, cert *x509.Certificate) (common.PKIidType, error)

	// InitializeChain initialize channel
	InitializeChain(chainMac common.ChainMac, chainState *protos.ChainState) error

	// CreateChain creates a channel
	CreateChain(chainMac common.ChainMac, chainID string, files []common.FileSyncInfo) (*protos.ChainState, error)

	// CloseChain closes a channel
	CloseChain(chainMac common.ChainMac, notify bool) error

	// CreateLeaveChainMessage creates LeaveChainMessage for channel
	CreateLeaveChainMessage(chainMac common.ChainMac) (*protos.SignedRKSyncMessage, error)

	// GetPeers returns the NetworkMembers considered alive
	Peers() []common.NetworkMember

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)

	// Stop the gossip component
	Stop()
}

// emittedRKSyncMessage encapsulates isgned rksync message to compose
// with routing filter to be used while message is forwarded
type emittedRKSyncMessage struct {
	*protos.SignedRKSyncMessage
	filter func(id common.PKIidType) bool
}
