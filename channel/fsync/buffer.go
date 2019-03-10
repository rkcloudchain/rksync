package fsync

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

// PayloadBuffer is used to store payloads into which used to
// support payloads with file blocks reordering according to the
// sequence number.
type PayloadBuffer interface {
	Push(payload *protos.Payload)
	Next() int64
	Pop() *protos.Payload
	Size() int
	Ready() chan struct{}
	Close()
}

type payloadBufferImpl struct {
	next      int64
	buf       map[int64]*protos.Payload
	readyChan chan struct{}
	mutex     sync.RWMutex
	stopChan  chan struct{}
}

// NewPayloadBuffer is factory function to create new payloads buffer
func NewPayloadBuffer(next int64) PayloadBuffer {
	b := &payloadBufferImpl{
		buf:       make(map[int64]*protos.Payload),
		readyChan: make(chan struct{}, 1),
		next:      next,
		stopChan:  make(chan struct{}),
	}

	go b.expirationRoutine()
	return b
}

func (b *payloadBufferImpl) Ready() chan struct{} {
	return b.readyChan
}

func (b *payloadBufferImpl) Push(payload *protos.Payload) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if payload.IsAppend() {
		metadata := payload.GetAppend()
		if metadata.Start < b.next || b.buf[metadata.Start] != nil {
			logging.Debugf("Payload with start number = %d has been already processed", metadata.Start)
			return
		}

		b.buf[metadata.Start] = payload

		if metadata.Start == b.next && len(b.readyChan) == 0 {
			b.readyChan <- struct{}{}
		}
	}
}

func (b *payloadBufferImpl) Next() int64 {
	return atomic.LoadInt64(&b.next)
}

func (b *payloadBufferImpl) Pop() *protos.Payload {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	payload := b.buf[b.Next()]

	if payload != nil {
		delete(b.buf, b.Next())
		if payload.IsAppend() {
			metadata := payload.GetAppend()
			atomic.AddInt64(&b.next, metadata.Length)
		}
		b.drainReadChannel()
		return payload
	}

	return nil
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
	close(b.stopChan)
}

func (b *payloadBufferImpl) isPurgeNeeded(shouldBePurged func(*protos.Payload) bool) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for _, m := range b.buf {
		if shouldBePurged(m) {
			return true
		}
	}
	return false
}

func (b *payloadBufferImpl) expirationRoutine() {
	for {
		select {
		case <-b.stopChan:
			return
		case <-time.After(5 * time.Second):
			hasMessageExpired := func(payload *protos.Payload) bool {
				if payload.IsAppend() {
					if payload.GetAppend().Start < atomic.LoadInt64(&b.next) {
						return true
					}
				}
				return false
			}
			if b.isPurgeNeeded(hasMessageExpired) {
				b.expireMessage()
			}
		}
	}
}

func (b *payloadBufferImpl) expireMessage() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for key, value := range b.buf {
		if value.IsAppend() {
			if value.GetAppend().Start < atomic.LoadInt64(&b.next) {
				delete(b.buf, key)
			}
		}
	}
}
