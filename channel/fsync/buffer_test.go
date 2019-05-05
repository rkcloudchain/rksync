/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsync

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func payloadWithStart(start int64) (*protos.Payload, error) {
	data := make([]byte, 64)
	n, err := rand.Read(data)
	if err != nil {
		return nil, err
	}
	return &protos.Payload{
		Data: data,
		Metadata: &protos.Payload_Append{
			Append: &protos.AppendMetadata{
				Start:  start,
				Length: int64(n),
			},
		},
	}, nil
}

func TestNewPayloadBuffer(t *testing.T) {
	buffer := NewPayloadBuffer(10)
	assert.Equal(t, int64(10), buffer.Next())
}

func TestPayloadBufferPush(t *testing.T) {
	buffer := NewPayloadBuffer(10)
	payload, err := payloadWithStart(9)
	require.NoError(t, err)

	buffer.Push(payload)
	assert.Equal(t, int64(10), buffer.Next())
	assert.Equal(t, 0, buffer.Size())

	payload, err = payloadWithStart(10)
	require.NoError(t, err)

	buffer.Push(payload)
	assert.Equal(t, int64(10), buffer.Next())
	assert.Equal(t, 1, buffer.Size())
}

func TestPayloadBufferReady(t *testing.T) {
	fin := make(chan struct{})
	buffer := NewPayloadBuffer(1)
	assert.Equal(t, int64(1), buffer.Next())

	go func() {
		<-buffer.Ready()
		fin <- struct{}{}
	}()

	time.AfterFunc(100*time.Millisecond, func() {
		payload, err := payloadWithStart(1)
		require.NoError(t, err)
		buffer.Push(payload)
	})

	select {
	case <-fin:
		payload := buffer.Peek()
		assert.Equal(t, int64(1), payload.GetAppend().Start)
	case <-time.After(500 * time.Millisecond):
		t.Fail()
	}
}

func TestPayloadBufferReset(t *testing.T) {
	buffer := NewPayloadBuffer(2)
	payload, err := payloadWithStart(5)
	require.NoError(t, err)

	buffer.Push(payload)
	assert.Equal(t, int64(2), buffer.Next())
	assert.Equal(t, 1, buffer.Size())

	buffer.Reset(2)
	assert.Equal(t, int64(4), buffer.Next())
	assert.Equal(t, 0, buffer.Size())
}

func TestPayloadBufferExpire(t *testing.T) {
	buffer := NewPayloadBuffer(3)
	payload, err := payloadWithStart(3)
	require.NoError(t, err)

	buffer.Push(payload)

	payload, err = payloadWithStart(6)
	require.NoError(t, err)

	buffer.Push(payload)
	assert.Equal(t, int64(3), buffer.Next())
	assert.Equal(t, 2, buffer.Size())

	payload, err = payloadWithStart(10)
	require.NoError(t, err)

	buffer.Push(payload)
	buffer.Expire(4)
	assert.Equal(t, int64(7), buffer.Next())
	assert.Equal(t, 1, buffer.Size())
}
