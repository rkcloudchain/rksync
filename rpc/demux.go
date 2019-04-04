/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"bytes"
	"sync"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

// ChannelDeMultiplexer is a struct that can receive channel registrations
type ChannelDeMultiplexer struct {
	channels []*channel
	seqNum   uint64
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
	mac  []byte
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

	if m.isClosed() {
		return nil
	}

	ch := &channel{ch: make(chan interface{}, 10), pred: predicate, mac: []byte{}}
	m.channels = append(m.channels, ch)
	return ch.ch
}

// AddChannelWithMAC registers a channel with a certain predicate and a byte slice
func (m *ChannelDeMultiplexer) AddChannelWithMAC(predicate common.MessageAcceptor, mac []byte) <-chan interface{} {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed() {
		return nil
	}

	ch := &channel{ch: make(chan interface{}, 10), pred: predicate, mac: mac}
	m.channels = append(m.channels, ch)
	return ch.ch
}

// Unregister closes a channel with a certain predicate
func (m *ChannelDeMultiplexer) Unregister(mac []byte) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed() {
		return
	}

	n := len(m.channels)
	for i := 0; i < n; i++ {
		ch := m.channels[i]
		if bytes.Equal(mac, ch.mac) {
			m.channels = append(m.channels[:i], m.channels[i+1:]...)
			n--
			i--
		}
	}
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
			ch.ch <- msg
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
