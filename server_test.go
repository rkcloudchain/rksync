package rksync

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateGossipConfig(t *testing.T) {
	cfg := &config.GossipConfig{}
	err := validateGossipConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "At least one bootstrap peer needs to be provided")

	cfg.BootstrapPeers = []string{"peer0.org1.rockontrol.com"}
	err = validateGossipConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Must specify the endpoint address of the peer")

	cfg.Endpoint = "peer1.org2.rockontrol.com"
	err = validateGossipConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Must specify the FileSystem interface")

	cfg.FileSystem = mocks.NewFSMock("base")
	err = validateGossipConfig(cfg)
	assert.NoError(t, err)
}

func TestRKSyncServiceServe(t *testing.T) {
	home, err := filepath.Abs("tests")
	require.NoError(t, err)

	cfg1 := &config.Config{
		HomeDir: filepath.Join(home, "fixtures", "identity", "peer0"),
		Gossip: &config.GossipConfig{
			FileSystem:     mocks.NewFSMock(filepath.Join(home, "testdata", "peer0")),
			BootstrapPeers: []string{"localhost:8053"},
			Endpoint:       "localhost:8053",
		},
		Identity: &config.IdentityConfig{
			ID: "peer0.org1",
		},
	}

	l1, err := net.Listen("tcp", "0.0.0.0:8053")
	require.NoError(t, err)

	srv1, err := Serve(l1, cfg1)
	assert.NoError(t, err)
	defer srv1.Stop()

	cfg2 := &config.Config{
		HomeDir: filepath.Join(home, "fixtures", "identity", "peer1"),
		Gossip: &config.GossipConfig{
			FileSystem:     mocks.NewFSMock(filepath.Join(home, "testdata", "peer1")),
			BootstrapPeers: []string{"localhost:8053"},
			Endpoint:       "localhost:9053",
		},
		Identity: &config.IdentityConfig{
			ID: "peer1.org2",
		},
	}

	l2, err := net.Listen("tcp", "0.0.0.0:9053")
	require.NoError(t, err)

	srv2, err := Serve(l2, cfg2)
	assert.NoError(t, err)
	defer srv2.Stop()

	cfg3 := &config.Config{
		HomeDir: filepath.Join(home, "fixtures", "identity", "peer2"),
		Gossip: &config.GossipConfig{
			FileSystem:     mocks.NewFSMock(filepath.Join(home, "testdata", "peer2")),
			BootstrapPeers: []string{"localhost:8053"},
			Endpoint:       "localhost:10053",
		},
		Identity: &config.IdentityConfig{
			ID: "peer2.org3",
		},
	}

	l3, err := net.Listen("tcp", "0.0.0.0:10053")
	require.NoError(t, err)

	srv3, err := Serve(l3, cfg3)
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	assert.Len(t, srv1.gossip.Peers(), 2)
	assert.Len(t, srv2.gossip.Peers(), 2)
	assert.Len(t, srv3.gossip.Peers(), 2)

	t.Log("Stop rksync service 3\n")
	srv3.Stop()
	time.Sleep(10 * time.Second)

	assert.Len(t, srv1.gossip.Peers(), 1)
	assert.Len(t, srv2.gossip.Peers(), 1)

	srv3, err = Serve(l3, cfg3)
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	assert.Len(t, srv1.gossip.Peers(), 2)
	assert.Len(t, srv2.gossip.Peers(), 2)
	assert.Len(t, srv3.gossip.Peers(), 2)
}
