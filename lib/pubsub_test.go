/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lib

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPubSub(t *testing.T) {
	ps := NewPubSub()
	sub1 := ps.Subscribe("test1", time.Second)
	sub2 := ps.Subscribe("test2", time.Second)

	assert.NotNil(t, sub1)

	go func() {
		err := ps.Publish("test1", 5)
		assert.NoError(t, err)
	}()

	item, err := sub1.Listen()
	assert.NoError(t, err)
	assert.Equal(t, 5, item)

	err = ps.Publish("test3", 5)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no subscribers")

	go func() {
		time.Sleep(time.Second * 2)
		ps.Publish("test2", 10)
	}()

	item, err = sub2.Listen()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Nil(t, item)

	subscriptions := []Subscription{}
	n := 100
	for i := 0; i < n; i++ {
		subscriptions = append(subscriptions, ps.Subscribe("test4", time.Second))
	}

	go func() {
		for i := 0; i <= subscriptionBuffSize; i++ {
			err := ps.Publish("test4", 100+i)
			assert.NoError(t, err)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(n)
	for _, s := range subscriptions {
		go func(s Subscription) {
			time.Sleep(time.Second)
			defer wg.Done()
			for i := 0; i < subscriptionBuffSize; i++ {
				item, err := s.Listen()
				assert.NoError(t, err)
				assert.Equal(t, 100+i, item)
			}

			item, err := s.Listen()
			assert.Nil(t, item)
			assert.Error(t, err)
		}(s)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		ps.Lock()
		empty := len(ps.subscriptions) == 0
		ps.Unlock()
		if empty {
			break
		}
	}

	ps.Lock()
	defer ps.Unlock()
	assert.Empty(t, ps.subscriptions)
}
