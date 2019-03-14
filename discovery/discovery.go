/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
)

// CryptoService is an interface that discovery expects to be implemented and passed on creation
type CryptoService interface {
	ValidateAliveMsg(message *protos.SignedRKSyncMessage) bool
	SignMessage(m *protos.RKSyncMessage) *protos.Envelope
}

// EnvelopeFilter may or may not remove part of the Envelope
// that the given SignedRKSyncMessage originates from.
type EnvelopeFilter func(message *protos.SignedRKSyncMessage) *protos.Envelope

// Sieve defines the messages that are allowed to be sent to some remote peer.
// based on some criteria.
// Returns whether the sieve permits sending a given message
type Sieve func(message *protos.SignedRKSyncMessage) bool

// DisclosurePolicy defines which  messages a given remote peer
// is eligible of knowing about, and also what is it eligible
// to know about out of a given SignedRKSyncMessage
type DisclosurePolicy func(remotePeer *common.NetworkMember) (Sieve, EnvelopeFilter)

// RPCService is an interface that the discovery expects to be implemented and passed on creation
type RPCService interface {
	Gossip(msg *protos.SignedRKSyncMessage)
	SendToPeer(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage)
	Ping(peer *common.NetworkMember) bool
	Accept() <-chan protos.ReceivedMessage
	PresumedDead() <-chan common.PKIidType
	CloseConn(peer *common.NetworkMember)
	Forward(msg protos.ReceivedMessage)
}

type identifier func() (common.PKIidType, error)

// Discovery is the interface represents a discovery module
type Discovery interface {
	// GetMembership returns the alive members in the view
	GetMembership() []common.NetworkMember

	// InitiateSync makes the instance ask a given number of peers
	// for their membership information
	InitiateSync(peerNum int)

	// Connect makes this instance to connect to a remote instance
	Connect(member common.NetworkMember, id identifier)

	// Lookup returns a network member, or nil if not found
	Lookup(pkiID common.PKIidType) *common.NetworkMember

	// Stop this instance
	Stop()
}
