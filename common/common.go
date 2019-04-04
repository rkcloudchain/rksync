/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// PKIidType defines the type that holds the PKI-id
// which is the security identifier of a peer
type PKIidType []byte

func (p PKIidType) String() string {
	if p == nil {
		return "<nil>"
	}
	return hex.EncodeToString(p)
}

// IsNotSameFilter generate filter function which provides
// a predicate to identify whenever current id
// equals to another one.
func (p PKIidType) IsNotSameFilter(that PKIidType) bool {
	return !bytes.Equal(p, that)
}

// PeerIdentityType is the peer's certificate
type PeerIdentityType []byte

// NetworkMember defines a peer's endpoint and its PKIid
type NetworkMember struct {
	Endpoint string
	PKIID    PKIidType
}

// String converts a NetworkMember to a string
func (p *NetworkMember) String() string {
	return fmt.Sprintf("%s, PKIid: %v", p.Endpoint, p.PKIID)
}

// MessageAcceptor is a predicate that is used to
// determine in which messages the subscriber that created the
// instance of the MessageAcceptor is interested in.
type MessageAcceptor func(interface{}) bool

// PeerSuspector returns whether a peer with a given identity is suspected
// as being revoked, or its CA is revoked
type PeerSuspector func(identity PeerIdentityType) bool

// MessageReplcaingPolicy returns:
// - MessageInvalidates if this message invalidates that
// - MessageInvalidated if this message is invalidated by that
// - MessageNoAction otherwise
type MessageReplcaingPolicy func(this interface{}, that interface{}) InvalidationResult

// InvalidationResult determines how a message offects another message
type InvalidationResult int

const (
	// MessageNoAction means messages have no relation
	MessageNoAction InvalidationResult = iota

	// MessageInvalidates means message invalidates the other message
	MessageInvalidates

	// MessageInvalidated means message is invalidated by the other message
	MessageInvalidated
)

// FileSyncInfo defines a file sync mode
type FileSyncInfo struct {
	Path     string
	Mode     string
	Metadata []byte
}

// ChainMac defines the identity representation of a chain
type ChainMac []byte

func (mac ChainMac) String() string {
	if mac == nil {
		return "<nil>"
	}

	return hex.EncodeToString(mac)
}
