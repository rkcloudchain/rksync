package protos

import (
	"bytes"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
)

// NewRKSyncMessageComparator creates a MessageReplcaingPolicy
func NewRKSyncMessageComparator() common.MessageReplcaingPolicy {
	return func(this interface{}, that interface{}) common.InvalidationResult {
		return invalidationPolicy(this, that)
	}
}

func invalidationPolicy(this interface{}, that interface{}) common.InvalidationResult {
	thisMsg := this.(*SignedRKSyncMessage)
	thatMsg := that.(*SignedRKSyncMessage)

	if thisMsg.IsAliveMsg() && thatMsg.IsAliveMsg() {
		return aliveInvalidationPolicy(thisMsg.GetAliveMsg(), thatMsg.GetAliveMsg())
	}
	if thisMsg.IsChainStateMsg() && thatMsg.IsChainStateMsg() {
		if !bytes.Equal(thisMsg.RKSyncMessage.Channel, thatMsg.RKSyncMessage.Channel) {
			return common.MessageNoAction
		}
		return stateInvalidationPolicy(thisMsg.GetState(), thatMsg.GetState())
	}

	return common.MessageNoAction
}

func aliveInvalidationPolicy(this *AliveMessage, that *AliveMessage) common.InvalidationResult {
	if !bytes.Equal(this.Membership.PkiId, that.Membership.PkiId) {
		return common.MessageNoAction
	}

	return compareTimestamps(this.Timestamp, that.Timestamp)
}

func stateInvalidationPolicy(this *ChainState, that *ChainState) common.InvalidationResult {
	if this.SeqNum > that.SeqNum {
		return common.MessageInvalidates
	}
	return common.MessageInvalidated
}

func compareTimestamps(thisTS *PeerTime, thatTS *PeerTime) common.InvalidationResult {
	if thisTS.IncNum == thatTS.IncNum {
		if thisTS.SeqNum > thatTS.SeqNum {
			return common.MessageInvalidates
		}

		return common.MessageInvalidated
	}

	if thisTS.IncNum < thatTS.IncNum {
		return common.MessageInvalidated
	}

	return common.MessageInvalidates
}

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

// SignedRKSyncMessage contains a RKSyncMessage
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

	payloadSigVerificationErr := verify(peerIdentity, m.Envelope.Signature, m.Envelope.Payload)
	if payloadSigVerificationErr != nil {
		return payloadSigVerificationErr
	}
	return nil
}

// IsSigned returns whether the message has a signature in the envelope
func (m *SignedRKSyncMessage) IsSigned() bool {
	return m.Envelope != nil && m.Envelope.Payload != nil && m.Envelope.Signature != nil
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

// IsAliveMsg returns whether this RKSyncMessage is an AliveMessage
func (m *RKSyncMessage) IsAliveMsg() bool {
	return m.GetAliveMsg() != nil
}

// IsChainStateMsg returns whether this RKSyncMessage is a chain state message
func (m *RKSyncMessage) IsChainStateMsg() bool {
	return m.GetState() != nil
}

// IsStatePullRequestMsg returns wether this RKSyncMessage is a state pull request
func (m *RKSyncMessage) IsStatePullRequestMsg() bool {
	return m.GetStatePullRequest() != nil
}

// IsStatePullResponseMsg returns wether this RKSyncMessage is a state pull response
func (m *RKSyncMessage) IsStatePullResponseMsg() bool {
	return m.GetStatePullResponse() != nil
}

// IsStateInfoMsg returns wether this RKSyncMessage is a state info message
func (m *RKSyncMessage) IsStateInfoMsg() bool {
	return m.GetStateInfo() != nil
}

// IsTagLegal checks the RKSyncMessage tags and inner type
func (m *RKSyncMessage) IsTagLegal() error {
	if m.IsAliveMsg() || m.GetMemReq() != nil || m.GetMemRes() != nil {
		if m.Tag != RKSyncMessage_EMPTY {
			return fmt.Errorf("Tag should be %s", RKSyncMessage_Tag_name[int32(RKSyncMessage_EMPTY)])
		}
		return nil
	}

	return fmt.Errorf("Unknown message type: %v", m)
}

// IsChannelRestricted returns whether this RKSyncMessage should be routed only in its channel
func (m *RKSyncMessage) IsChannelRestricted() bool {
	return m.Tag == RKSyncMessage_CHAN_ONLY
}

// GetChainStateInfo ...
func (m *ChainState) GetChainStateInfo() (*ChainStateInfo, error) {
	msg, err := m.Envelope.ToRKSyncMessage()
	if err != nil {
		return nil, err
	}
	if !msg.RKSyncMessage.IsStateInfoMsg() {
		return nil, errors.New("Failed to unmarshal ChainStateInfo message")
	}

	return msg.GetStateInfo(), nil
}

// Sign signs a ChainStateInfo with given Signer.
func (si *ChainStateInfo) Sign(signer Signer) (*Envelope, error) {
	payload, err := proto.Marshal(si)
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

	return e, nil
}
