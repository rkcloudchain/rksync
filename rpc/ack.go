package rpc

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/protos"
)

func interceptAcks(nextHandler handler, remotePeerID common.PKIidType, pubSub *lib.PubSub) func(*protos.SignedRKSyncMessage) {
	return func(m *protos.SignedRKSyncMessage) {
		if m.IsAck() {
			topic := topicForAck(m.Nonce, remotePeerID)
			pubSub.Publish(topic, m.GetAck())
			return
		}
		nextHandler(m)
	}
}
