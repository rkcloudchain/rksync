package common

import "encoding/hex"

// PKIidType defines the type that holds the PKI-id
// which is the security identifier of a peer
type PKIidType []byte

func (p PKIidType) String() string {
	if p == nil {
		return "<nil>"
	}
	return hex.EncodeToString(p)
}

// PeerIdentityType is the peer's certificate
type PeerIdentityType []byte

// NetworkMember defines a peer's endpoint and its PKIid
type NetworkMember struct {
	Endpoint string
	PKIID    PKIidType
}

// MessageAcceptor is a predicate that is used to
// determine in which messages the subscriber that created the
// instance of the MessageAcceptor is interested in.
type MessageAcceptor func(interface{}) bool
