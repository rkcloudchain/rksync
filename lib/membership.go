/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lib

import (
	"sync"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
)

// MembershipStore struct which encapsulates
// membership message store abstraction
type MembershipStore struct {
	m map[string]*protos.SignedRKSyncMessage
	sync.RWMutex
}

// NewMembershipStore creates new membership store instance
func NewMembershipStore() *MembershipStore {
	return &MembershipStore{m: make(map[string]*protos.SignedRKSyncMessage)}
}

// MsgByID returns a message stored by a certain ID, or nil
// if such an ID isn't found
func (m *MembershipStore) MsgByID(pkiID common.PKIidType) *protos.SignedRKSyncMessage {
	m.RLock()
	defer m.RUnlock()
	if msg, exists := m.m[pkiID.String()]; exists {
		return msg
	}
	return nil
}

// Size of the membership store
func (m *MembershipStore) Size() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.m)
}

// Put associates msg with the given PKI-ID
func (m *MembershipStore) Put(pkiID common.PKIidType, msg *protos.SignedRKSyncMessage) {
	m.Lock()
	defer m.Unlock()
	m.m[pkiID.String()] = msg
}

// Remove removes a message with a given pkiID
func (m *MembershipStore) Remove(pkiID common.PKIidType) {
	m.Lock()
	defer m.Unlock()
	delete(m.m, pkiID.String())
}

// ToSlice returns a slice backed by the elements
// of the MembershipStore
func (m *MembershipStore) ToSlice() []*protos.SignedRKSyncMessage {
	m.RLock()
	defer m.RUnlock()
	members := make([]*protos.SignedRKSyncMessage, len(m.m))
	i := 0
	for _, member := range m.m {
		members[i] = member
		i++
	}
	return members
}
