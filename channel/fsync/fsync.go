/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsync

import (
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

const (
	dataBlockSize = 512 * 1024
)

// Adapter enables the fsync to communicate with rksync channel
type Adapter interface {
	GetFileSystem() config.FileSystem
	SendToPeer(*protos.SignedRKSyncMessage, *common.NetworkMember)
	Lookup(common.PKIidType) *common.NetworkMember
	Sign(*protos.RKSyncMessage) (*protos.SignedRKSyncMessage, error)
	GetMembership() []common.NetworkMember
	IsMemberInChan(common.NetworkMember) bool
	Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage)
}

// FileSyncProvider is the file synchronization interface
type FileSyncProvider interface {
	Stop()
}

// NewFileSyncProvider creates FileSyncProvier instance
func NewFileSyncProvider(chainMac common.ChainMac, chainID string, filename string, metadata []byte, mode protos.File_Mode, leader bool,
	pkiID common.PKIidType, adapter Adapter) (FileSyncProvider, error) {

	if mode == protos.File_Append {
		return createAppendSyncProvider(chainMac, chainID, filename, metadata, leader, pkiID, adapter)
	}

	if mode == protos.File_Random {
		return createRandomSyncProvider(chainMac, chainID, filename, metadata, leader, pkiID, adapter)
	}

	return nil, errors.Errorf("Unknow file sync mode: %d", mode)
}

// GenerateMAC returns a byte slice that is derived from the channel's mac
// and a file name
func GenerateMAC(chainMac []byte, filename string) []byte {
	raw := append(chainMac, []byte(filename)...)
	return util.ComputeSHA3256(raw)
}
