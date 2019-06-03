/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsync

import (
	"sync"
	"sync/atomic"

	"github.com/rkcloudchain/rksync/protos"
)

// PayloadBuffer is used to store payloads into which used to
// support payloads with file blocks reordering according to the
// sequence number.
type PayloadBuffer interface {
	Push(payload *protos.DataMessage)
	Next() int64
	Expire(delta int64)
	Peek() *protos.DataMessage
	Reset(delta int64)
	Size() int
	Ready() chan struct{}
	Close()
}

type payloadBufferImpl struct {
	next      int64
	buf       map[int64]*protos.DataMessage
	readyChan chan struct{}
	mutex     sync.RWMutex
}

// NewPayloadBuffer is factory function to create new payloads buffer
func NewPayloadBuffer(next int64) PayloadBuffer {
	b := &payloadBufferImpl{
		buf:       make(map[int64]*protos.DataMessage),
		readyChan: make(chan struct{}, 1),
		next:      next,
	}

	return b
}

func (b *payloadBufferImpl) Ready() chan struct{} {
	return b.readyChan
}

func (b *payloadBufferImpl) Push(message *protos.DataMessage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if message.IsAppend() {
		payload := message.GetAppend()
		if payload.StartOffset < b.next {
			return
		}
		if b.buf[payload.StartOffset] == nil {
			b.buf[payload.StartOffset] = message
		}

		if payload.StartOffset == b.next && len(b.readyChan) == 0 {
			b.readyChan <- struct{}{}
		}
	}
}

func (b *payloadBufferImpl) Next() int64 {
	return atomic.LoadInt64(&b.next)
}

func (b *payloadBufferImpl) Peek() *protos.DataMessage {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.buf[b.Next()]
}

func (b *payloadBufferImpl) Expire(delta int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	atomic.AddInt64(&b.next, delta)

	b.expireMessage()
	b.drainReadChannel()
}

func (b *payloadBufferImpl) Reset(delta int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	atomic.AddInt64(&b.next, delta)
	b.buf = make(map[int64]*protos.DataMessage)
	b.drainReadChannel()
}

func (b *payloadBufferImpl) drainReadChannel() {
	if len(b.buf) == 0 {
		for {
			if len(b.readyChan) > 0 {
				<-b.readyChan
			} else {
				break
			}
		}
	}
}

func (b *payloadBufferImpl) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.buf)
}

func (b *payloadBufferImpl) Close() {
	close(b.readyChan)
}

func (b *payloadBufferImpl) expireMessage() {
	for key, value := range b.buf {
		if value.IsAppend() {
			if value.GetAppend().StartOffset < atomic.LoadInt64(&b.next) {
				delete(b.buf, key)
			}
		}
	}
}
