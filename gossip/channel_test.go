/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/mocks"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestAddSelfToChainMembers(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:12053"}, "localhost:12053", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "testchannel")
	_, err = gossipSvc1.CreateChain(mac, "testchannel", []*common.FileSyncInfo{})
	assert.NoError(t, err)

	_, err = gossipSvc1.AddMemberToChain(mac, gossipSvc1.SelfPKIid())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Can't add self-node to the channel members")
}

func TestChannelInit(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9053"}, "localhost:9053", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	gossipSvc2, err := CreateGossipServer([]string{"localhost:9053"}, "localhost:10053", 1)
	require.NoError(t, err)
	defer gossipSvc2.Stop()

	time.Sleep(5 * time.Second)
	fmt.Printf("Svc1 members: %+v\n", gossipSvc1.Peers())
	assert.Equal(t, 1, len(gossipSvc1.Peers()))
	fmt.Printf("Svc2 members: %+v\n", gossipSvc2.Peers())
	assert.Equal(t, 1, len(gossipSvc2.Peers()))

	fmt.Println("Create channel")
	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "testchannel")
	_, err = gossipSvc1.CreateChain(mac, "testchannel", []*common.FileSyncInfo{})
	assert.NoError(t, err)
	fmt.Println("Add member to channel")
	_, err = gossipSvc1.AddMemberToChain(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	selfChannelInfo := gossipSvc2.SelfChainInfo("testchannel")
	assert.NotNil(t, selfChannelInfo)
	msg, err := selfChannelInfo.Envelope.ToRKSyncMessage()
	assert.NoError(t, err)
	chainStateInfo := msg.GetStateInfo()
	assert.NotNil(t, chainStateInfo)

	assert.Equal(t, gossipSvc1.SelfPKIid(), common.PKIidType(chainStateInfo.Leader))
	assert.Equal(t, 2, len(chainStateInfo.Properties.Members))
}

func TestChannelClose(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9055"}, "localhost:9055", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	gossipSvc2, err := CreateGossipServer([]string{"localhost:9055"}, "localhost:10055", 1)
	require.NoError(t, err)
	defer gossipSvc2.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "closechannel")
	_, err = gossipSvc1.CreateChain(mac, "closechannel", []*common.FileSyncInfo{})
	assert.NoError(t, err)

	_, err = gossipSvc1.AddMemberToChain(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	chainInfo := gossipSvc2.SelfChainInfo("closechannel")
	assert.NotNil(t, chainInfo)

	time.Sleep(5 * time.Second)
	err = gossipSvc1.CloseChain(mac, true)
	assert.NoError(t, err)

	time.Sleep(3 * time.Second)
	chainInfo = gossipSvc2.SelfChainInfo("closechannel")
	assert.Nil(t, chainInfo)
}

func TestCreateLeaveChainMessage(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9055"}, "localhost:9055", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "channel1")
	assert.NotNil(t, mac)
	msg, err := gossipSvc1.CreateLeaveChainMessage(mac)
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.NotNil(t, msg.RKSyncMessage)
	assert.NotNil(t, msg.Envelope)
	assert.NotNil(t, msg.Envelope.Payload)
	assert.NotNil(t, msg.Envelope.Signature)
	assert.True(t, len(msg.Envelope.Signature) > 0)
}

func TestRemoveMemberWithChain(t *testing.T) {
	gossipSvc1, err := CreateGossipServer([]string{"localhost:9058"}, "localhost:9058", 0)
	require.NoError(t, err)
	defer gossipSvc1.Stop()

	gossipSvc2, err := CreateGossipServer([]string{"localhost:9058"}, "localhost:10058", 1)
	require.NoError(t, err)
	defer gossipSvc2.Stop()

	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "channel3")
	_, err = gossipSvc1.CreateChain(mac, "channel3", []*common.FileSyncInfo{})
	assert.NoError(t, err)

	_, err = gossipSvc1.AddMemberToChain(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	chainState := gossipSvc2.SelfChainInfo("channel3")
	assert.NotNil(t, chainState)

	gossipSvc3, err := CreateGossipServer([]string{"localhost:9058"}, "localhost:8058", 2)
	require.NoError(t, err)
	defer gossipSvc3.Stop()

	_, err = gossipSvc1.AddMemberToChain(mac, gossipSvc3.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	chainState = gossipSvc3.SelfChainInfo("channel3")
	assert.NotNil(t, chainState)

	chainState, err = gossipSvc1.RemoveMemberWithChain(mac, []byte{0})
	assert.NoError(t, err)
	chainInfo, err := chainState.GetChainStateInfo()
	assert.NoError(t, err)
	assert.Len(t, chainInfo.Properties.Members, 3)

	_, err = gossipSvc2.RemoveMemberWithChain(mac, gossipSvc3.SelfPKIid())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed verifying ChainStateInfo message")

	chainState, err = gossipSvc1.RemoveMemberWithChain(mac, gossipSvc3.SelfPKIid())
	assert.NoError(t, err)
	chainInfo, err = chainState.GetChainStateInfo()
	assert.NoError(t, err)
	assert.Len(t, chainInfo.Properties.Members, 2)

	time.Sleep(5 * time.Second)

	chainState = gossipSvc3.SelfChainInfo("channel3")
	assert.Nil(t, chainState)
}

func secureDialOpts() []grpc.DialOption {
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxCallSendMsgSize(config.MaxSendMsgSize),
	))

	dialOpts = append(dialOpts, config.ClientKeepaliveOptions(nil)...)
	dialOpts = append(dialOpts, grpc.WithInsecure())

	return dialOpts
}

// CreateGossipServer creates a gossip server
func CreateGossipServer(bootstrap []string, address string, num int) (Gossip, error) {
	home, err := filepath.Abs(fmt.Sprintf("../tests/fixtures/identity/peer%d", num))
	if err != nil {
		return nil, err
	}

	idCfg := &config.IdentityConfig{
		ID: fmt.Sprintf("peer%d.org%d", num, num+1),
	}
	err = idCfg.MakeFilesAbs(home)
	if err != nil {
		return nil, err
	}

	selfIdentity, err := util.GetIdentity(idCfg)
	if err != nil {
		return nil, err
	}

	gsrv, err := CreateGRPCServer(address)
	if err != nil {
		return nil, err
	}

	cfg := util.DefaultGossipConfig(bootstrap, address)
	p, err := filepath.Abs("../tests/testdata")
	if err != nil {
		return nil, err
	}
	cfg.FileSystem = mocks.NewFSMock(filepath.Join(p, fmt.Sprintf("peer%d", num)))

	gossipSrv, err := NewGossipService(cfg, idCfg, gsrv.Server(), selfIdentity, secureDialOpts)
	if err != nil {
		return nil, err
	}

	go gsrv.Start()
	return gossipSrv, nil
}

// CreateGRPCServer creates a new grpc server
func CreateGRPCServer(address string) (*server.GRPCServer, error) {
	return server.NewGRPCServer(address, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
}
