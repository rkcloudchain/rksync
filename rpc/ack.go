package rpc

import (
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
)

func interceptAcks(nextHandler handler, remotePeerID common.PKIidType) func(*protos.SignedRKSyncMessage) {
	return nil
}
