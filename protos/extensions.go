/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protos

import (
	"bytes"
	"encoding/hex"
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
		if !bytes.Equal(thisMsg.RKSyncMessage.ChainMac, thatMsg.RKSyncMessage.ChainMac) {
			return common.MessageNoAction
		}
		return stateInvalidationPolicy(thisMsg.GetState(), thatMsg.GetState())
	}
	if thisMsg.IsChainPullRequestMsg() && thatMsg.IsChainPullRequestMsg() {
		if !bytes.Equal(thisMsg.RKSyncMessage.ChainMac, thatMsg.RKSyncMessage.ChainMac) {
			return common.MessageNoAction
		}
		return compareTimestamps(thisMsg.GetStatePullRequest().Timestamp, thatMsg.GetStatePullRequest().Timestamp)
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
	env := "No envelope"
	if m.Envelope != nil {
		env = fmt.Sprintf("%d bytes, Signature: %d bytes %s", len(m.Envelope.Payload), len(m.Envelope.Signature), hex.EncodeToString(m.Envelope.Signature))
	}

	msg := "No rksync message"
	if m.RKSyncMessage != nil {
		if m.IsAliveMsg() {
			msg = aliveMessageToString(m.GetAliveMsg())
		} else if m.IsChainStateMsg() {
			msg = chainStatMessageToString(m.RKSyncMessage.ChainMac, m.GetState())
		} else {
			msg = m.RKSyncMessage.String()
		}
	}

	return fmt.Sprintf("RKSyncMessage: %s, Envelope: %s", msg, env)
}

func chainStatMessageToString(chainMac []byte, cs *ChainState) string {
	str := fmt.Sprintf("chain_state_message: Channel MAC: %s, Sequence: %d", hex.EncodeToString(chainMac), cs.SeqNum)
	msg, err := cs.Envelope.ToRKSyncMessage()
	if err == nil {
		if msg.IsStateInfoMsg() {
			str = fmt.Sprintf("%s, StateInfo: %s", str, chainStateInfoToString(msg.GetStateInfo()))
		}
	}
	return str
}

func chainStateInfoToString(csi *ChainStateInfo) string {
	return fmt.Sprintf("Leader: %s, Properties: %s", string(csi.Leader), chainStateInfoPropertyToString(csi.Properties))
}

func chainStateInfoPropertyToString(p *Properties) string {
	buf := bytes.NewBufferString("Members: ")
	for _, member := range p.Members {
		buf.WriteString(string(member) + ",")
	}

	buf.WriteString("Files: ")
	for _, file := range p.Files {
		buf.WriteString(fmt.Sprintf("Path-%s, Mode: %s;", file.Path, File_Mode_name[int32(file.Mode)]))
	}

	return buf.String()
}

func aliveMessageToString(am *AliveMessage) string {
	if am.Membership == nil {
		return "nil membership"
	}
	var si string
	serializeIdentity := &SerializedIdentity{}
	if err := proto.Unmarshal(am.Identity, serializeIdentity); err == nil {
		si = serializeIdentity.NodeId + string(serializeIdentity.IdBytes)
	}
	return fmt.Sprintf("Alive message: %s, Identity: %s, Timestamp: %s", membershipToString(am.Membership), si, am.Timestamp)
}

func membershipToString(m *Member) string {
	return fmt.Sprintf("Membership: Endpoint: %s, PKI-id: %s", m.Endpoint, m.PkiId)
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

// IsChainPullRequestMsg returns whether this RKSyncMessage is an ChainStatePullRequest message
func (m *RKSyncMessage) IsChainPullRequestMsg() bool {
	return m.GetStatePullRequest() != nil
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

// IsDataMsg returns whether this RKSyncMessage is a data message
func (m *RKSyncMessage) IsDataMsg() bool {
	return m.GetDataMsg() != nil
}

// IsDataReq returns whether this RKSyncMessage is a data request
func (m *RKSyncMessage) IsDataReq() bool {
	return m.GetDataReq() != nil
}

// IsTagLegal checks the RKSyncMessage tags and inner type
func (m *RKSyncMessage) IsTagLegal() error {
	if m.IsAliveMsg() || m.GetMemReq() != nil || m.GetMemRes() != nil {
		if m.Tag != RKSyncMessage_EMPTY {
			return fmt.Errorf("Tag should be %s", RKSyncMessage_Tag_name[int32(RKSyncMessage_EMPTY)])
		}
		return nil
	}
	if m.IsDataMsg() || m.IsDataReq() || m.IsChainStateMsg() || m.IsStatePullRequestMsg() || m.IsStatePullResponseMsg() {
		if m.Tag != RKSyncMessage_CHAN_ONLY {
			return fmt.Errorf("Tag should be %s", RKSyncMessage_Tag_name[int32(RKSyncMessage_CHAN_ONLY)])
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

// IsAppend returns whether this Payload is a append message
func (p *Payload) IsAppend() bool {
	return p.GetAppend() != nil
}

// IsAppend returns whether this DataRequest is a append message
func (r *DataRequest) IsAppend() bool {
	return r.GetAppend() != nil
}
