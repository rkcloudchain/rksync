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
