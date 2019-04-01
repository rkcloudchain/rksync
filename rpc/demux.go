/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

// ChannelDeMultiplexer is a struct that can receive channel registrations
type ChannelDeMultiplexer struct {
	channels []*channel
	lock     sync.RWMutex
	closed   bool
}

// NewChannelDemultiplexer creates a new ChannelDeMultiplexer
func NewChannelDemultiplexer() *ChannelDeMultiplexer {
	return &ChannelDeMultiplexer{channels: make([]*channel, 0)}
}

type channel struct {
	pred common.MessageAcceptor
	ch   chan interface{}
}

func (m *ChannelDeMultiplexer) isClosed() bool {
	return m.closed
}

// Close closes this channel, which makes all channels registered before
// to close as well
func (m *ChannelDeMultiplexer) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.closed = true
	for _, ch := range m.channels {
		close(ch.ch)
	}
	m.channels = nil
}

// AddChannel registers a channel with a certain predicate
func (m *ChannelDeMultiplexer) AddChannel(predicate common.MessageAcceptor) <-chan interface{} {
	m.lock.Lock()
	defer m.lock.Unlock()

	ch := &channel{ch: make(chan interface{}, 10), pred: predicate}
	m.channels = append(m.channels, ch)
	return ch.ch
}

// DeMultiplex broadcasts the message to all channels that were returned
// by AddChannel calls and that hold the respected predicates.
func (m *ChannelDeMultiplexer) DeMultiplex(msg interface{}) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if m.isClosed() {
		return
	}
	for _, ch := range m.channels {
		if ch.pred(msg) {
			go func(c *channel) {
				c.ch <- msg
			}(ch)
		}
	}
}

// ReceivedMessageImpl is an implementation of ReceiveMessage
type ReceivedMessageImpl struct {
	*protos.SignedRKSyncMessage
	conn     *connection
	connInfo *protos.ConnectionInfo
}

// GetSourceEnvelope returns the Envelope the ReceiveMessage was constructed with
func (m *ReceivedMessageImpl) GetSourceEnvelope() *protos.Envelope {
	return m.Envelope
}

// Respond sends a msg to the source that sent the ReceiveMessage
func (m *ReceivedMessageImpl) Respond(msg *protos.RKSyncMessage) {
	sMsg, err := msg.NoopSign()
	if err != nil {
		err = errors.WithStack(err)
		logging.Errorf("Failed creating SignedRKSyncMessage: %+v", err)
		return
	}
	m.conn.send(sMsg, func(e error) {}, true)
}

// GetRKSyncMessage returns the inner RKSyncMessage
func (m *ReceivedMessageImpl) GetRKSyncMessage() *protos.SignedRKSyncMessage {
	return m.SignedRKSyncMessage
}

// GetConnectionInfo returns information about the remote peer
func (m *ReceivedMessageImpl) GetConnectionInfo() *protos.ConnectionInfo {
	return m.connInfo
}

// Ack returns to the sender an acknowledgement for the message
func (m *ReceivedMessageImpl) Ack(err error) {
	ackMsg := &protos.RKSyncMessage{
		Nonce: m.GetRKSyncMessage().Nonce,
		Content: &protos.RKSyncMessage_Ack{
			Ack: &protos.Acknowledgement{},
		},
	}
	if err != nil {
		ackMsg.GetAck().Error = err.Error()
	}
	m.Respond(ackMsg)
}
