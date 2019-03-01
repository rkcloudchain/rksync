package protos

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
)

// ReceivedMessage is a RKSyncMessage wrapper that
// enables the user to send a message to the origin from which
// the ReceivedMessage was sent from.
type ReceivedMessage interface {
	// Respond sends a RKSyncMessage to the origin from which this ReceivedMessage was sent from
	Respond(msg *RKSyncMessage)

	// GetRKSyncMessage returns the underlying RKSyncMessage
	GetRKSyncMessage() *SignedRKSyncMessage

	// GetSourceMessage Returns the Envelope the ReceivedMessage was
	// constructed with
	GetSourceEnvelope() *Envelope

	// GetConnectionInfo returns information about the remote peer
	// that sent the message
	GetConnectionInfo() *ConnectionInfo

	// Ack returns to the sender an acknowledgement for the message
	// An ack can receive an error that indicates that the operation related
	// to the message has failed
	Ack(err error)
}

// SignedRKSyncMessage contains a GossipMessage
// and the Envelope from which it came from
type SignedRKSyncMessage struct {
	*Envelope
	*RKSyncMessage
}

// String returns a string representation of a SignedRKSyncMessage
func (m *SignedRKSyncMessage) String() string {
	return ""
}

// ConnectionInfo represents information about the remote peer
type ConnectionInfo struct {
	ID       common.PKIidType
	Identity common.PeerIdentityType
	Endpoint string
}

// String returns a string representation of this ConnectionInfo
func (c *ConnectionInfo) String() string {
	return fmt.Sprintf("%s %v", c.Endpoint, c.ID)
}

// Verifier receives a peer identity, a signature and a message
// and returns nil if the signature on the message could be verified
// using the given identity
type Verifier func(peerIdentity []byte, signature, message []byte) error

// Signer signs a message, and returns (signature, nil)
// on success, and nil and an error on failure
type Signer func(msg []byte) ([]byte, error)

// ToRKSyncMessage unmarshals a given envelope and creates a SignedRKSyncMessage
// out of it.
func (e *Envelope) ToRKSyncMessage() (*SignedRKSyncMessage, error) {
	if e == nil {
		return nil, errors.New("nil envelope")
	}

	msg := &RKSyncMessage{}
	err := proto.Unmarshal(e.Payload, msg)
	if err != nil {
		return nil, errors.Errorf("Failed unmarshaling GossipMessage from envelope: %v", err)
	}

	return &SignedRKSyncMessage{
		RKSyncMessage: msg,
		Envelope:      e,
	}, nil
}

// Sign signs a RKSyncMessage with given Signer.
func (m *SignedRKSyncMessage) Sign(signer Signer) (*Envelope, error) {
	m.Envelope = nil
	payload, err := proto.Marshal(m.RKSyncMessage)
	if err != nil {
		return nil, err
	}

	sig, err := signer(payload)
	if err != nil {
		return nil, err
	}

	e := &Envelope{
		Payload:   payload,
		Signature: sig,
	}
	m.Envelope = e
	return e, nil
}

// Verify verifies a signed RKSyncMessage with a given Verifier.
func (m *SignedRKSyncMessage) Verify(peerIdentity []byte, verify Verifier) error {
	if m.Envelope == nil {
		return errors.New("Missing envelope")
	}
	if len(m.Envelope.Payload) == 0 {
		return errors.New("Empty payload")
	}
	if len(m.Envelope.Signature) == 0 {
		return errors.New("Empty signature")
	}

	payloadSigVerificationErr := verify(peerIdentity, m.Envelope.Payload, m.Envelope.Signature)
	if payloadSigVerificationErr != nil {
		return payloadSigVerificationErr
	}
	return nil
}

// NoopSign creates a SignedRKSyncMessage with a nil signature
func (m *RKSyncMessage) NoopSign() (*SignedRKSyncMessage, error) {
	signer := func(msg []byte) ([]byte, error) {
		return nil, nil
	}
	sMsg := &SignedRKSyncMessage{RKSyncMessage: m}
	_, err := sMsg.Sign(signer)
	return sMsg, err
}

// IsAck returns whether this RKSyncMessage is an acknowledgement
func (m *RKSyncMessage) IsAck() bool {
	return m.GetAck() != nil
}
