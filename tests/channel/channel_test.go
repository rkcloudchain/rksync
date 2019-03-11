package channel

import (
	"fmt"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/gossip"
	"github.com/rkcloudchain/rksync/tests/runner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestChannelInit(t *testing.T) {
	selfIdentity1, _ := runner.GetIdentity(runner.GetOrg1IdentityConfig())
	selfIdentity2, _ := runner.GetIdentity(runner.GetOrg2IdentityConfig())

	srv1, _ := runner.CreateGRPCServer("localhost:9053")
	srv2, _ := runner.CreateGRPCServer("localhost:10053")

	gossipSvc1, err := gossip.NewGossipService(runner.DefaultGossipConfig("localhost:9053"), runner.GetOrg1IdentityConfig(), srv1.Server(), selfIdentity1, secureDialOpts)
	require.NoError(t, err)
	go srv1.Start()
	defer gossipSvc1.Stop()

	gossipSvc2, err := gossip.NewGossipService(runner.DefaultGossipConfig("localhost:10053"), runner.GetOrg2IdentityConfig(), srv2.Server(), selfIdentity2, secureDialOpts)
	require.NoError(t, err)
	go srv2.Start()
	defer gossipSvc2.Stop()

	time.Sleep(5 * time.Second)
	fmt.Printf("Svc1 members: %+v\n", gossipSvc1.Peers())
	assert.Equal(t, 1, len(gossipSvc1.Peers()))
	fmt.Printf("Svc2 members: %+v\n", gossipSvc2.Peers())
	assert.Equal(t, 1, len(gossipSvc2.Peers()))

	fmt.Println("Create channel")
	_, err = gossipSvc1.CreateChannel("testchannel", []common.FileSyncInfo{})
	assert.NoError(t, err)
	fmt.Println("Add member to channel")
	_, err = gossipSvc1.AddMemberToChan("testchannel", gossipSvc2.SelfPKIid())
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
