/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lib

import (
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
)

func TestMembershipStore(t *testing.T) {
	store := NewMembershipStore()

	id1 := common.PKIidType("id1")
	id2 := common.PKIidType("id2")

	msg1 := &protos.SignedRKSyncMessage{}
	msg2 := &protos.SignedRKSyncMessage{Envelope: &protos.Envelope{}}

	assert.Nil(t, store.MsgByID(id1))
	assert.Equal(t, 0, store.Size())

	store.Put(id1, msg1)
	assert.NotNil(t, store.MsgByID(id1))

	store.Put(id2, msg2)
	assert.Equal(t, msg1, store.MsgByID(id1))
	assert.NotEqual(t, msg2, store.MsgByID(id1))

	assert.Equal(t, 2, store.Size())

	store.Remove(id1)
	assert.Nil(t, store.MsgByID(id1))
	assert.Equal(t, 1, store.Size())

	msg3 := &protos.SignedRKSyncMessage{RKSyncMessage: &protos.RKSyncMessage{}}
	msg3Clone := &protos.SignedRKSyncMessage{RKSyncMessage: &protos.RKSyncMessage{}}
	id3 := common.PKIidType("id3")
	store.Put(id3, msg3)
	assert.Equal(t, msg3Clone, msg3)
	store.MsgByID(id3).Channel = []byte{0, 1, 2, 3}
	assert.NotEqual(t, msg3Clone, msg3)
}

func TestToSlice(t *testing.T) {
	store := NewMembershipStore()
	id1 := common.PKIidType("id1")
	id2 := common.PKIidType("id2")
	id3 := common.PKIidType("id3")
	id4 := common.PKIidType("id4")

	msg1 := &protos.SignedRKSyncMessage{}
	msg2 := &protos.SignedRKSyncMessage{Envelope: &protos.Envelope{}}
	msg3 := &protos.SignedRKSyncMessage{RKSyncMessage: &protos.RKSyncMessage{}}
	msg4 := &protos.SignedRKSyncMessage{RKSyncMessage: &protos.RKSyncMessage{}, Envelope: &protos.Envelope{}}

	store.Put(id1, msg1)
	store.Put(id2, msg2)
	store.Put(id3, msg3)
	store.Put(id4, msg4)

	assert.Len(t, store.ToSlice(), 4)

	existsInSlice := func(slice []*protos.SignedRKSyncMessage, msg *protos.SignedRKSyncMessage) bool {
		for _, m := range slice {
			if assert.ObjectsAreEqual(m, msg) {
				return true
			}
		}
		return false
	}

	expectedMsgs := []*protos.SignedRKSyncMessage{msg1, msg2, msg3, msg4}
	for _, msg := range store.ToSlice() {
		assert.True(t, existsInSlice(expectedMsgs, msg))
	}
}
