/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var idCfg1 *config.IdentityConfig
var idCfg2 *config.IdentityConfig

func TestMain(m *testing.M) {
	home1, err := filepath.Abs("../tests/fixtures/identity/peer0")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	idCfg1 = &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = idCfg1.MakeFilesAbs(home1)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	home2, err := filepath.Abs("../tests/fixtures/identity/peer1")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	idCfg2 = &config.IdentityConfig{
		ID: "peer1.org2",
	}
	err = idCfg2.MakeFilesAbs(home2)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	os.Exit(m.Run())
}

func TestChannelInit(t *testing.T) {
	selfIdentity1, _ := util.GetIdentity(idCfg1)
	selfIdentity2, _ := util.GetIdentity(idCfg2)

	srv1, _ := CreateGRPCServer("localhost:9053")
	srv2, _ := CreateGRPCServer("localhost:10053")

	gossipSvc1, err := NewGossipService(util.DefaultGossipConfig("localhost:9053"), idCfg1, srv1.Server(), selfIdentity1, secureDialOpts)
	require.NoError(t, err)
	go srv1.Start()
	defer gossipSvc1.Stop()

	gossipSvc2, err := NewGossipService(util.DefaultGossipConfig("localhost:10053"), idCfg2, srv2.Server(), selfIdentity2, secureDialOpts)
	require.NoError(t, err)
	go srv2.Start()
	defer gossipSvc2.Stop()

	time.Sleep(5 * time.Second)
	fmt.Printf("Svc1 members: %+v\n", gossipSvc1.Peers())
	assert.Equal(t, 1, len(gossipSvc1.Peers()))
	fmt.Printf("Svc2 members: %+v\n", gossipSvc2.Peers())
	assert.Equal(t, 1, len(gossipSvc2.Peers()))

	fmt.Println("Create channel")
	mac := channel.GenerateMAC(gossipSvc1.SelfPKIid(), "testchannel")
	_, err = gossipSvc1.CreateChannel(mac, "testchannel", []common.FileSyncInfo{})
	assert.NoError(t, err)
	fmt.Println("Add member to channel")
	_, err = gossipSvc1.AddMemberToChan(mac, gossipSvc2.SelfPKIid())
	assert.NoError(t, err)

	time.Sleep(5 * time.Second)
	selfChannelInfo := gossipSvc2.SelfChannelInfo("testchannel")
	assert.NotNil(t, selfChannelInfo)
	msg, err := selfChannelInfo.Envelope.ToRKSyncMessage()
	assert.NoError(t, err)
	chainStateInfo := msg.GetStateInfo()
	assert.NotNil(t, chainStateInfo)

	assert.Equal(t, gossipSvc1.SelfPKIid(), common.PKIidType(chainStateInfo.Leader))
	assert.Equal(t, 2, len(chainStateInfo.Properties.Members))
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

// CreateGRPCServer creates a new grpc server
func CreateGRPCServer(address string) (*server.GRPCServer, error) {
	return server.NewGRPCServer(address, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
}
