package channel

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/gossip"
	"github.com/rkcloudchain/rksync/tests/runner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSync(t *testing.T) {
	selfIdentity1, _ := runner.GetIdentity(runner.GetOrg1IdentityConfig())
	selfIdentity2, _ := runner.GetIdentity(runner.GetOrg2IdentityConfig())

	srv1, _ := runner.CreateGRPCServer("localhost:9053")
	srv2, _ := runner.CreateGRPCServer("localhost:10053")

	cfg1 := runner.DefaultGossipConfig("localhost:9053")
	cfg1.FileSystem = &mockFileSystem{baseDir: "/Users/xuqiaolun/Downloads/org1"}
	gossipSvc1, err := gossip.NewGossipService(cfg1, runner.GetOrg1IdentityConfig(), srv1.Server(), selfIdentity1, secureDialOpts)
	require.NoError(t, err)
	go srv1.Start()
	defer gossipSvc1.Stop()

	cfg2 := runner.DefaultGossipConfig("localhost:10053")
	cfg2.FileSystem = &mockFileSystem{baseDir: "/Users/xuqiaolun/Downloads/org2"}
	gossipSvc2, err := gossip.NewGossipService(cfg2, runner.GetOrg2IdentityConfig(), srv2.Server(), selfIdentity2, secureDialOpts)
	require.NoError(t, err)
	go srv2.Start()
	defer gossipSvc2.Stop()

	_, err = gossipSvc1.CreateChannel("testchannel", []common.FileSyncInfo{})
	assert.NoError(t, err)
	_, err = gossipSvc1.AddMemberToChan("testchannel", gossipSvc2.SelfPKIid())
	assert.NoError(t, err)
	_, err = gossipSvc1.AddFileToChan("testchannel", common.FileSyncInfo{Path: "ShadowsocksX-NG.app.1.8.2.zip", Mode: "Append"})
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	selfChannelInfo := gossipSvc2.SelfChannelInfo("testchannel")
	require.NotNil(t, selfChannelInfo)
	msg, err := selfChannelInfo.Envelope.ToRKSyncMessage()
	assert.NoError(t, err)
	chainStateInfo := msg.GetStateInfo()
	assert.NotNil(t, chainStateInfo)

	assert.Equal(t, gossipSvc1.SelfPKIid(), common.PKIidType(chainStateInfo.Leader))
	assert.Equal(t, 2, len(chainStateInfo.Properties.Members))
	assert.Equal(t, 1, len(chainStateInfo.Properties.Files))

	time.Sleep(10 * time.Second)
}

type mockFileSystem struct {
	baseDir string
}

func (m *mockFileSystem) Create(chainID, filename string) (config.File, error) {
	p := filepath.Join(m.baseDir, chainID, filename)
	dir := filepath.Dir(p)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	f, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (m *mockFileSystem) OpenFile(chainID, filename string, flag int, perm os.FileMode) (config.File, error) {
	p := filepath.Join(m.baseDir, chainID, filename)
	return os.OpenFile(p, flag, perm)
}

func (m *mockFileSystem) Stat(chainID, filename string) (os.FileInfo, error) {
	p := filepath.Join(m.baseDir, chainID, filename)
	return os.Stat(p)
}
