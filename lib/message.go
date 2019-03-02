package lib

import (
	"sync"
	"time"

	"github.com/rkcloudchain/rksync/common"
)

var noopLock = func() {}

type invalidationTrigger func(message interface{})

// MessageStore adds message to an internal buffer
type MessageStore interface {
	Add(msg interface{}) bool
	CheckValid(msg interface{}) bool
	Size() int
	Get() []interface{}
	Stop()
	Purge(func(interface{}) bool)
}

// NewMessageStore returns a new MessageStore with the message replacing
func NewMessageStore(pol common.MessageReplcaingPolicy, trigger invalidationTrigger) MessageStore {
	return newMsgStore(pol, trigger)
}

// NewMessageStoreExpirable returns a new MessageStore with the message replacing
// It supports old message expiration after msgTTL
func NewMessageStoreExpirable(pol common.MessageReplcaingPolicy, trigger invalidationTrigger,
	msgTTL time.Duration, externalLock func(), externalUnlock func(), externalExpire func(interface{})) MessageStore {

	store := newMsgStore(pol, trigger)
	store.msgTTL = msgTTL

	if externalLock != nil {
		store.externalLock = externalLock
	}
	if externalUnlock != nil {
		store.externalUnlock = externalUnlock
	}
	if externalExpire != nil {
		store.expireMsgCallback = externalExpire
	}

	go store.expirationRoutine()
	return store
}

func newMsgStore(pol common.MessageReplcaingPolicy, trigger invalidationTrigger) *messageStoreImpl {
	return &messageStoreImpl{
		pol:               pol,
		messages:          make([]*msg, 0),
		invTrigger:        trigger,
		externalLock:      noopLock,
		externalUnlock:    noopLock,
		expireMsgCallback: func(m interface{}) {},
		expiredCount:      0,
		doneCh:            make(chan struct{}),
	}
}

type messageStoreImpl struct {
	pol               common.MessageReplcaingPolicy
	lock              sync.RWMutex
	messages          []*msg
	invTrigger        invalidationTrigger
	msgTTL            time.Duration
	expiredCount      int
	externalLock      func()
	externalUnlock    func()
	expireMsgCallback func(msg interface{})
	doneCh            chan struct{}
	stopOnce          sync.Once
}

type msg struct {
	data    interface{}
	created time.Time
	expired bool
}

func (s *messageStoreImpl) Add(message interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	n := len(s.messages)
	for i := 0; i < n; i++ {
		m := s.messages[i]
		switch s.pol(message, m.data) {
		case common.MessageInvalidated:
			return false
		case common.MessageInvalidates:
			s.invTrigger(m.data)
			s.messages = append(s.messages[:i], s.messages[i+1:]...)
			n--
			i--
		}
	}

	s.messages = append(s.messages, &msg{data: message, created: time.Now()})
	return true
}

func (s *messageStoreImpl) Purge(should func(interface{}) bool) {
	shouldBePurged := func(m *msg) bool {
		return should(m.data)
	}
	if !s.isPurgeNeeded(shouldBePurged) {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	n := len(s.messages)
	for i := 0; i < n; i++ {
		if !shouldBePurged(s.messages[i]) {
			continue
		}
		s.invTrigger(s.messages[i].data)
		s.messages = append(s.messages[:i], s.messages[i+1:]...)
		n--
		i--
	}
}

func (s *messageStoreImpl) CheckValid(message interface{}) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, m := range s.messages {
		if s.pol(message, m.data) == common.MessageInvalidated {
			return false
		}
	}
	return true
}

func (s *messageStoreImpl) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.messages) - s.expiredCount
}

func (s *messageStoreImpl) Get() []interface{} {
	res := make([]interface{}, 0)

	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, m := range s.messages {
		if !m.expired {
			res = append(res, m.data)
		}
	}
	return res
}

func (s *messageStoreImpl) Stop() {
	stopFunc := func() {
		close(s.doneCh)
	}
	s.stopOnce.Do(stopFunc)
}

func (s *messageStoreImpl) expireMessages() {
	s.externalLock()
	s.lock.Lock()
	defer s.lock.Unlock()
	defer s.externalUnlock()

	n := len(s.messages)
	for i := 0; i < n; i++ {
		m := s.messages[i]
		if !m.expired {
			if time.Since(m.created) > s.msgTTL {
				m.expired = true
				s.expireMsgCallback(m.data)
				s.expiredCount++
			}
		} else {
			if time.Since(m.created) > (s.msgTTL * 2) {
				s.messages = append(s.messages[:i], s.messages[i+1:]...)
				n--
				i--
				s.expiredCount--
			}
		}
	}
}

func (s *messageStoreImpl) isPurgeNeeded(should func(*msg) bool) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for _, m := range s.messages {
		if should(m) {
			return true
		}
	}
	return false
}

func (s *messageStoreImpl) expirationRoutine() {
	for {
		select {
		case <-s.doneCh:
			return
		case <-time.After(s.expirationCheckInterval()):
			hasMessageExpired := func(m *msg) bool {
				if !m.expired && time.Since(m.created) > s.msgTTL {
					return true
				} else if time.Since(m.created) > (s.msgTTL * 2) {
					return true
				}
				return false
			}

			if s.isPurgeNeeded(hasMessageExpired) {
				s.expireMessages()
			}
		}
	}
}

func (s *messageStoreImpl) expirationCheckInterval() time.Duration {
	return s.msgTTL / 100
}
