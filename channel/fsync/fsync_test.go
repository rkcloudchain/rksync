package fsync_test

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/channel/fsync"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	channelA      = "A"
	pkiIDForPeer1 = common.PKIidType("peer1")
	pkiIDForPeer2 = common.PKIidType("peer2")
)

type dummyFile struct{}

func (f *dummyFile) Close() error {
	return nil
}

func (f *dummyFile) ReadAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}

func (f *dummyFile) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (f *dummyFile) Read(p []byte) (n int, err error) {
	return len(p), nil
}

func (f *dummyFile) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

type dummyFileInfo struct{}

func (fi *dummyFileInfo) Name() string {
	return ""
}

func (fi *dummyFileInfo) Size() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(100)
}

func (fi *dummyFileInfo) Mode() os.FileMode {
	return os.ModePerm
}

func (fi *dummyFileInfo) ModTime() time.Time {
	return time.Now()
}

func (fi *dummyFileInfo) IsDir() bool {
	return false
}

func (fi *dummyFileInfo) Sys() interface{} {
	return nil
}

type dummyFileSystem struct {
	t      *testing.T
	leader bool
}

func (fs *dummyFileSystem) Create(chainID string, meta config.FileMeta) (config.File, error) {
	return &dummyFile{}, nil
}

func (fs *dummyFileSystem) OpenFile(chainID string, fmeta config.FileMeta, flag int, perm os.FileMode) (config.File, error) {
	return &dummyFile{}, nil
}

func (fs *dummyFileSystem) Stat(chainID string, fmeta config.FileMeta) (os.FileInfo, error) {
	assert.Equal(fs.t, fs.leader, fmeta.Leader)
	return &dummyFileInfo{}, nil
}

func (fs *dummyFileSystem) Chtimes(chainID string, fmeta config.FileMeta, mtime time.Time) error {
	return nil
}

type dummyRPCModule struct {
	fs *dummyFileSystem
	mock.Mock
}

func (m *dummyRPCModule) GetFileSystem() config.FileSystem {
	return m.fs
}

func (m *dummyRPCModule) SendToPeer(msg *protos.SignedRKSyncMessage, peer *common.NetworkMember) {
	if !m.wasMocked("SendToPeer") {
		return
	}
	m.Called(msg, peer)
}

func (m *dummyRPCModule) Lookup(pkiID common.PKIidType) *common.NetworkMember {
	if !m.wasMocked("Lookup") {
		return &common.NetworkMember{}
	}
	args := m.Called(pkiID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*common.NetworkMember)
}

func (m *dummyRPCModule) Sign(msg *protos.RKSyncMessage) (*protos.SignedRKSyncMessage, error) {
	return msg.NoopSign()
}

func (m *dummyRPCModule) GetMembership() []common.NetworkMember {
	args := m.Called()
	members := args.Get(0).([]common.NetworkMember)
	return members
}

func (m *dummyRPCModule) IsMemberInChan(member common.NetworkMember) bool {
	return true
}

func (m *dummyRPCModule) Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage) {
	args := m.Called(acceptor, mac, passThrough)
	return args.Get(0).(<-chan *protos.RKSyncMessage), args.Get(1).(<-chan protos.ReceivedMessage)
}

func (m *dummyRPCModule) wasMocked(methodName string) bool {
	m.On("bla", mock.Anything)
	for _, ec := range m.ExpectedCalls {
		if ec.Method == methodName {
			return true
		}
	}
	return false
}

func TestNewFileSyncProvider(t *testing.T) {
	adapter := new(dummyRPCModule)
	chainMac := channel.GenerateMAC(pkiIDForPeer1, channelA)

	msgChan := make(<-chan *protos.RKSyncMessage)
	reqChan := make(<-chan protos.ReceivedMessage)
	adapter.On("Accept", mock.Anything, mock.Anything, mock.Anything).Return(msgChan, reqChan)

	fs := &dummyFileSystem{t: t, leader: false}
	adapter.fs = fs

	_, err := fsync.NewFileSyncProvider(chainMac, channelA, "filename", []byte{}, protos.File_Append, false, pkiIDForPeer1, adapter)
	assert.NoError(t, err)
}
