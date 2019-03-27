package gossip

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSync(t *testing.T) {
	home, err := filepath.Abs("../tests/testdata")
	require.NoError(t, err)
	require.NotEmpty(t, home)

	selfIdentity1, _ := util.GetIdentity(idCfg1)
	selfIdentity2, _ := util.GetIdentity(idCfg2)

	srv1, _ := CreateGRPCServer("localhost:9053")
	srv2, _ := CreateGRPCServer("localhost:10053")

	cfg1 := util.DefaultGossipConfig("localhost:9053")
	cfg1.FileSystem = &mockFileSystem{baseDir: filepath.Join(home, "peer0")}
	gossipSvc1, err := NewGossipService(cfg1, idCfg1, srv1.Server(), selfIdentity1, secureDialOpts)
	require.NoError(t, err)
	go srv1.Start()
	defer gossipSvc1.Stop()

	cfg2 := util.DefaultGossipConfig("localhost:10053")
	cfg2.FileSystem = &mockFileSystem{baseDir: filepath.Join(home, "peer1")}
	gossipSvc2, err := NewGossipService(cfg2, idCfg2, srv2.Server(), selfIdentity2, secureDialOpts)
	require.NoError(t, err)
	go srv2.Start()
	defer gossipSvc2.Stop()

	_, err = gossipSvc1.CreateChannel("testchannel", []common.FileSyncInfo{
		common.FileSyncInfo{Path: "101.png", Mode: "Append"},
		common.FileSyncInfo{Path: "config.yaml", Mode: "Append"},
		common.FileSyncInfo{Path: "rfc2616.txt", Mode: "Append"},
	})
	assert.NoError(t, err)
	_, err = gossipSvc1.AddMemberToChan("testchannel", gossipSvc2.SelfPKIid())
	assert.NoError(t, err)
	_, err = gossipSvc1.AddFileToChan("testchannel", common.FileSyncInfo{Path: "https-cert.pem", Mode: "Append"})
	assert.NoError(t, err)
	_, err = gossipSvc1.AddFileToChan("testchannel", common.FileSyncInfo{Path: "https-key.pem", Mode: "Append"})
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
	assert.Equal(t, 5, len(chainStateInfo.Properties.Files))

	time.Sleep(10 * time.Second)
}

type mockFileSystem struct {
	baseDir string
}

func (m *mockFileSystem) Create(chainID, filename string) (config.File, error) {
	p := filepath.Join(m.baseDir, filename)
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
	p := filepath.Join(m.baseDir, filename)
	return os.OpenFile(p, flag, perm)
}

func (m *mockFileSystem) Stat(chainID, filename string) (os.FileInfo, error) {
	p := filepath.Join(m.baseDir, filename)
	return os.Stat(p)
}
