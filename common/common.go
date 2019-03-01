package common

import (
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
