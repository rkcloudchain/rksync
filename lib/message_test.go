/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lib

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/stretchr/testify/assert"
)

func alwaysNoAction(_ interface{}, _ interface{}) common.InvalidationResult {
	return common.MessageNoAction
}

func compareInts(this interface{}, that interface{}) common.InvalidationResult {
	a := this.(int)
	b := that.(int)

	if a == b {
		return common.MessageNoAction
	}

	if a > b {
		return common.MessageInvalidates
	}

	return common.MessageInvalidated
}

func nonReplaceInts(this interface{}, that interface{}) common.InvalidationResult {
	a := this.(int)
	b := that.(int)
	if a == b {
		return common.MessageInvalidated
	}

	return common.MessageNoAction
}

func TestSize(t *testing.T) {
	msgStore := NewMessageStore(alwaysNoAction, Noop)
	msgStore.Add(1)
	msgStore.Add(2)
	msgStore.Add(3)
	assert.Equal(t, 3, msgStore.Size())
}

func TestConcurrency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	stopFlag := int32(0)
	msgStore := NewMessageStore(compareInts, Noop)
	looper := func(f func()) func() {
		return func() {
			for {
				if atomic.LoadInt32(&stopFlag) == int32(1) {
					return
				}
				f()
			}
		}
	}

	addProcess := looper(func() {
		msgStore.Add(rand.Int())
	})

	getProcess := looper(func() {
		msgStore.Get()
	})

	sizeProcess := looper(func() {
		msgStore.Size()
	})

	go addProcess()
	go getProcess()
	go sizeProcess()

	time.Sleep(time.Duration(3) * time.Second)
	atomic.CompareAndSwapInt32(&stopFlag, 0, 1)
}

func TestMessageGet(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	contains := func(a []interface{}, e interface{}) bool {
		for _, v := range a {
			if v == e {
				return true
			}
		}
		return false
	}

	msgStore := NewMessageStore(alwaysNoAction, Noop)
	expected := []int{}
	for i := 0; i < 2; i++ {
		n := rand.Int()
		expected = append(expected, n)
		msgStore.Add(n)
	}

	for _, num := range expected {
		assert.True(t, contains(msgStore.Get(), num))
	}
}

func TestMessageAdd(t *testing.T) {
	msgStore := NewMessageStore(compareInts, Noop)
	assert.True(t, msgStore.Add(10))
	assert.True(t, msgStore.Add(11))
	assert.False(t, msgStore.Add(9))

	assert.Equal(t, 1, msgStore.Size())
	assert.Equal(t, 11, msgStore.Get()[0].(int))
}

func TestExpiration(t *testing.T) {
	expired := make([]int, 0)

	msgStore := NewMessageStoreExpirable(nonReplaceInts, Noop, time.Second*3, nil, nil, func(m interface{}) {
		expired = append(expired, m.(int))
	})

	for i := 0; i < 10; i++ {
		assert.True(t, msgStore.Add(i))
	}

	assert.Equal(t, 10, msgStore.Size())

	time.Sleep(2 * time.Second)

	for i := 0; i < 10; i++ {
		assert.False(t, msgStore.Add(i))
		assert.False(t, msgStore.CheckValid(i))
	}

	for i := 10; i < 20; i++ {
		assert.True(t, msgStore.CheckValid(i))
		assert.True(t, msgStore.Add(i))
		assert.False(t, msgStore.CheckValid(i))
	}

	assert.Equal(t, 20, msgStore.Size())

	time.Sleep(2 * time.Second)

	for i := 0; i < 20; i++ {
		assert.False(t, msgStore.Add(i))
	}

	assert.Equal(t, 10, msgStore.Size())
	assert.Equal(t, 10, len(expired))

	time.Sleep(4 * time.Second)

	assert.Equal(t, 0, msgStore.Size())
	assert.Equal(t, 20, len(expired))

	for i := 0; i < 10; i++ {
		assert.True(t, msgStore.CheckValid(i))
		assert.True(t, msgStore.Add(i))
		assert.False(t, msgStore.CheckValid(i))
	}

	assert.Equal(t, 10, msgStore.Size())
}

func TestPurge(t *testing.T) {
	purged := make(chan int, 5)
	msgStore := NewMessageStore(alwaysNoAction, func(o interface{}) {
		purged <- o.(int)
	})

	for i := 0; i < 10; i++ {
		assert.True(t, msgStore.Add(i))
	}

	msgStore.Purge(func(o interface{}) bool {
		return o.(int) > 9
	})

	assert.Len(t, msgStore.Get(), 10)

	msgStore.Purge(func(o interface{}) bool {
		return o.(int)%2 == 0
	})

	assert.Len(t, msgStore.Get(), 5)

	for _, o := range msgStore.Get() {
		assert.Equal(t, 1, o.(int)%2)
	}

	close(purged)
	i := 0
	for n := range purged {
		assert.Equal(t, i, n)
		i += 2
	}
}

func TestStop(t *testing.T) {
	expired := make([]int, 0)
	msgStore := NewMessageStoreExpirable(nonReplaceInts, Noop, 3*time.Second, nil, nil, func(m interface{}) {
		expired = append(expired, m.(int))
	})

	for i := 0; i < 10; i++ {
		assert.True(t, msgStore.Add(i))
	}

	assert.Equal(t, 10, msgStore.Size())

	msgStore.Stop()

	time.Sleep(4 * time.Second)

	assert.Equal(t, 10, msgStore.Size())
	assert.Equal(t, 0, len(expired))

	msgStore.Stop()
}
