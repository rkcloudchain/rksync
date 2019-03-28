package rksync

import (
	"path/filepath"
	"testing"

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

func TestRKSyncServiceCreate(t *testing.T) {
	home, err := filepath.Abs("tests/fixtures/identity/peer0")
	require.NoError(t, err)

	cfg := &config.Config{
		BindPort: 9055,
		HomeDir:  home,
		Gossip: &config.GossipConfig{
			FileSystem:     mocks.NewFSMock("base"),
			BootstrapPeers: []string{"localhost:9053"},
			Endpoint:       "localhost:9053",
		},
		Identity: &config.IdentityConfig{},
	}
	_, err = New(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Node id must be provided")

	cfg.Identity.ID = "peer0.org1"
	_, err = New(cfg)
	assert.NoError(t, err)
}
