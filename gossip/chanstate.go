package gossip

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/discovery"
	"github.com/rkcloudchain/rksync/protos"
)

func newChannelState(g *gossipService) *channelState {
	return &channelState{
		stopping: int32(0),
		channels: make(map[string]channel.Channel),
		g:        g,
	}
}

type channelState struct {
	stopping int32
	sync.RWMutex
	channels map[string]channel.Channel
	g        *gossipService
}

func (cs *channelState) stop() {
	if cs.isStopping() {
		return
	}

	atomic.StoreInt32(&cs.stopping, int32(1))
	cs.Lock()
	defer cs.Unlock()
	for _, gc := range cs.channels {
		gc.Stop()
	}
}

func (cs *channelState) isStopping() bool {
	return atomic.LoadInt32(&cs.stopping) == int32(1)
}

func (cs *channelState) lookupChannelForMsg(msg protos.ReceivedMessage) channel.Channel {
	if msg.GetRKSyncMessage().IsStatePullRequestMsg() {
		spr := msg.GetRKSyncMessage().GetStatePullRequest()
		mac := spr.ChainMac
		pkiID := msg.GetConnectionInfo().ID
		return cs.getChannelByMAC(mac, pkiID)
	}
	return cs.lookupChannelForRKSyncMsg(msg.GetRKSyncMessage().RKSyncMessage, msg.GetConnectionInfo().ID)
}

func (cs *channelState) lookupChannelForRKSyncMsg(msg *protos.RKSyncMessage, pkiID common.PKIidType) channel.Channel {
	if !msg.IsChainStateMsg() {
		return cs.getChannelByChainID(string(msg.Channel))
	}
	chainStateMsg := msg.GetState()
	return cs.getChannelByMAC(chainStateMsg.ChainMac, pkiID)
}

func (cs *channelState) getChannelByMAC(receivedMAC []byte, pkiID common.PKIidType) channel.Channel {
	cs.RLock()
	defer cs.RUnlock()
	for chanName, gc := range cs.channels {
		mac := channel.GenerateMAC(pkiID, chanName)
		if bytes.Equal(mac, receivedMAC) {
			return gc
		}
	}
	return nil
}

func (cs *channelState) getChannelByChainID(chainID string) channel.Channel {
	if cs.isStopping() {
		return nil
	}
	cs.RLock()
	defer cs.RUnlock()
	return cs.channels[chainID]
}

func (cs *channelState) createChannel(chainID string, leader bool) channel.Channel {
	if cs.isStopping() {
		return nil
	}
	cs.Lock()
	defer cs.Unlock()

	gc, exists := cs.channels[chainID]
	if !exists {
		pkiID := cs.g.selfPKIid
		ga := &gossipAdapterImpl{gossipService: cs.g, Discovery: cs.g.disc}
		gc = channel.NewGossipChannel(pkiID, chainID, leader, ga, cs.g.idMapper)
		cs.channels[chainID] = gc
	}
	return gc
}

type gossipAdapterImpl struct {
	*gossipService
	discovery.Discovery
}

func (ga *gossipAdapterImpl) GetChannelConfig() config.ChannelConfig {
	return config.ChannelConfig{
		ID:                          ga.conf.ID,
		PublishStateInfoInterval:    ga.conf.PublishStateInfoInterval,
		PullPeerNum:                 ga.conf.PullPeerNum,
		PullInterval:                ga.conf.PullInterval,
		RequestStateInfoInterval:    ga.conf.RequestStateInfoInterval,
		StateInfoCacheSweepInterval: ga.conf.PullInterval * 5,
	}
}

func (ga *gossipAdapterImpl) Gossip(msg *protos.SignedRKSyncMessage) {
	ga.gossipService.emitter.Add(&emittedRKSyncMessage{
		SignedRKSyncMessage: msg,
		filter:              func(_ common.PKIidType) bool { return true },
	})
}

func (ga *gossipAdapterImpl) Forward(msg protos.ReceivedMessage) {
	ga.gossipService.emitter.Add(&emittedRKSyncMessage{
		SignedRKSyncMessage: msg.GetRKSyncMessage(),
		filter:              msg.GetConnectionInfo().ID.IsNotSameFilter,
	})
}

func (ga *gossipAdapterImpl) Send(msg *protos.SignedRKSyncMessage, peers ...*common.NetworkMember) {
	ga.gossipService.srv.Send(msg, peers...)
}
