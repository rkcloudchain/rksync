/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/discovery"
	"github.com/rkcloudchain/rksync/logging"
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
	logging.Info("Stopping channelState")
	defer logging.Info("Stopped channelState")
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
	if cs.isStopping() {
		return nil
	}
	cs.RLock()
	defer cs.RUnlock()

	m := msg.GetRKSyncMessage()
	mac := common.ChainMac(m.ChainMac)
	return cs.channels[mac.String()]
}

func (cs *channelState) getChannelByMAC(chainMac common.ChainMac) channel.Channel {
	if cs.isStopping() {
		return nil
	}
	cs.RLock()
	defer cs.RUnlock()

	return cs.channels[chainMac.String()]
}

func (cs *channelState) getChannelByChainID(chainID string) channel.Channel {
	if cs.isStopping() {
		return nil
	}
	cs.RLock()
	defer cs.RUnlock()

	for key, gc := range cs.channels {
		chainState := gc.Self()
		chainInfo, err := chainState.GetChainStateInfo()
		if err != nil {
			logging.Warningf("Failed getting ChainStateInfo message: %s", err)
			continue
		}

		mac := channel.GenerateMAC(chainInfo.Leader, chainID)
		expectedMac, _ := hex.DecodeString(key)

		if bytes.Equal(mac, expectedMac) {
			return gc
		}
	}

	return nil
}

func (cs *channelState) closeChannel(chainMac common.ChainMac) bool {
	if cs.isStopping() {
		return false
	}
	cs.Lock()
	defer cs.Unlock()

	gc, exists := cs.channels[chainMac.String()]
	if exists {
		gc.Stop()
		delete(cs.channels, chainMac.String())
		return true
	}

	return false
}

func (cs *channelState) joinChannel(chainMac common.ChainMac, chainID string, leader bool) channel.Channel {
	if cs.isStopping() {
		return nil
	}
	cs.Lock()
	defer cs.Unlock()

	gc, exists := cs.channels[chainMac.String()]
	if !exists {
		pkiID := cs.g.selfPKIid
		ga := &gossipAdapterImpl{gossipService: cs.g, Discovery: cs.g.disc}
		gc = channel.NewGossipChannel(pkiID, chainMac, chainID, leader, ga, cs.g.idMapper)
		cs.channels[chainMac.String()] = gc
	}
	return gc
}

type gossipAdapterImpl struct {
	*gossipService
	discovery.Discovery
}

func (ga *gossipAdapterImpl) GetChannelConfig() channel.Config {
	return channel.Config{
		FileSystem:                  ga.conf.FileSystem,
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

func (ga *gossipAdapterImpl) SendWithAck(msg *protos.SignedRKSyncMessage, timeout time.Duration, minAck int, peers ...*common.NetworkMember) error {
	results := ga.gossipService.srv.SendWithAck(msg, timeout, minAck, peers...)
	if results.AckCount() < minAck {
		return errors.New(results.String())
	}
	return nil
}
