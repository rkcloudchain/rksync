/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSync(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9054"}, "localhost:9054", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	gossipSvc2, err := CreateGossipServer([]string{"localhost:9054"}, "localhost:10054", 1)
	require.NoError(t, err)
	defer gossipSvc2.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "testchannel")
	_, err = gossipSvc1.CreateChannel(mac, "testchannel", []common.FileSyncInfo{
		common.FileSyncInfo{Path: "101.png", Mode: "Append"},
		common.FileSyncInfo{Path: "config.yaml", Mode: "Append"},
		common.FileSyncInfo{Path: "rfc2616.txt", Mode: "Append"},
	})
	assert.NoError(t, err)
	_, err = gossipSvc1.AddMemberToChan(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)
	_, err = gossipSvc1.AddFileToChan(mac, common.FileSyncInfo{Path: "https-cert.pem", Mode: "Append"})
	assert.NoError(t, err)
	_, err = gossipSvc1.AddFileToChan(mac, common.FileSyncInfo{Path: "https-key.pem", Mode: "Append"})
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

func TestChainStateDynamicUpdate(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9054"}, "localhost:9054", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "testchain")
	_, err = gossipSvc1.CreateChannel(mac, "testchain", []common.FileSyncInfo{
		common.FileSyncInfo{Path: "101.png", Mode: "Append"},
		common.FileSyncInfo{Path: "config.yaml", Mode: "Append"},
		common.FileSyncInfo{Path: "rfc2616.txt", Mode: "Append"},
	})
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	gossipSvc2, err := CreateGossipServer([]string{"localhost:9054"}, "localhost:10054", 1)
	require.NoError(t, err)
	defer gossipSvc2.Stop()

	chain := gossipSvc2.SelfChannelInfo("testchain")
	assert.Nil(t, chain)

	_, err = gossipSvc1.AddMemberToChan(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	chain = gossipSvc2.SelfChannelInfo("testchain")
	assert.NotNil(t, chain)

	msg, err := chain.Envelope.ToRKSyncMessage()
	assert.NoError(t, err)

	state := msg.GetStateInfo()
	assert.NotNil(t, state)
	assert.Len(t, state.Properties.Members, 2)
	assert.Len(t, state.Properties.Files, 3)

	_, err = gossipSvc1.AddFileToChan(mac, common.FileSyncInfo{Path: "https-cert.pem", Mode: "Append"})
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)

	chain = gossipSvc2.SelfChannelInfo("testchain")
	assert.NotNil(t, chain)

	msg, err = chain.Envelope.ToRKSyncMessage()
	assert.NoError(t, err)

	state = msg.GetStateInfo()
	assert.NotNil(t, state)
	assert.Len(t, state.Properties.Members, 2)
	assert.Len(t, state.Properties.Files, 4)
}
