/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"sync"
	"sync/atomic"

	"github.com/rkcloudchain/rksync/channel/fsync"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
)

func newFSyncState(gc *gossipChannel) *fsyncState {
	return &fsyncState{
		files:    make(map[string]*fsync.FileSyncProvier),
		gc:       gc,
		stopping: int32(0),
	}
}

type fsyncState struct {
	sync.RWMutex
	files    map[string]*fsync.FileSyncProvier
	gc       *gossipChannel
	stopping int32
}

func (f *fsyncState) lookupFSyncProviderByFilename(filename string) *fsync.FileSyncProvier {
	if f.isStopping() {
		return nil
	}

	f.RLock()
	defer f.RUnlock()
	return f.files[filename]
}

func (f *fsyncState) closeFSyncProvider(filename string) {
	if f.isStopping() {
		return
	}

	f.Lock()
	defer f.Unlock()

	if fp, exists := f.files[filename]; exists {
		fp.Stop()
	}
}

func (f *fsyncState) snapshot() []string {
	if f.isStopping() {
		return nil
	}

	f.RLock()
	defer f.RUnlock()

	fnames := make([]string, 0)
	for key := range f.files {
		fnames = append(fnames, key)
	}

	return fnames
}

func (f *fsyncState) createProvider(filename string, mode protos.File_Mode, metadata []byte, leader bool) error {
	if f.isStopping() {
		return nil
	}
	f.Lock()
	defer f.Unlock()

	_, exists := f.files[filename]
	if !exists {
		pkiID := f.gc.pkiID
		chainMac := f.gc.chainMac
		chainID := f.gc.chainID
		fa := &fsyncAdapterImpl{gossipChannel: f.gc}
		fs, err := fsync.NewFileSyncProvider(chainMac, chainID, filename, metadata, mode, leader, pkiID, fa)
		if err != nil {
			return err
		}

		f.files[filename] = fs
	}

	return nil
}

func (f *fsyncState) stop() {
	if f.isStopping() {
		return
	}

	atomic.StoreInt32(&f.stopping, int32(1))
	f.Lock()
	defer f.Unlock()
	for _, fs := range f.files {
		fs.Stop()
	}
}

func (f *fsyncState) isStopping() bool {
	return atomic.LoadInt32(&f.stopping) == int32(1)
}

type fsyncAdapterImpl struct {
	*gossipChannel
}

func (fa *fsyncAdapterImpl) GetFileSystem() config.FileSystem {
	return fa.gossipChannel.fs
}

func (fa *fsyncAdapterImpl) SendToPeer(message *protos.SignedRKSyncMessage, peer *common.NetworkMember) {
	fa.Send(message, peer)
}

func (fa *fsyncAdapterImpl) Sign(message *protos.RKSyncMessage) (*protos.SignedRKSyncMessage, error) {
	signer := func(msg []byte) ([]byte, error) {
		return fa.idMapper.Sign(msg)
	}

	signedMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: message,
	}
	envp, err := signedMsg.Sign(signer)
	if err != nil {
		return nil, err
	}

	return &protos.SignedRKSyncMessage{Envelope: envp, RKSyncMessage: message}, nil
}
