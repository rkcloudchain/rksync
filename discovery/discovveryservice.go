package discovery

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

const defaultHelloInterval = time.Duration(5) * time.Second

type gossipDiscoveryService struct {
	self              common.NetworkMember
	seqNum            uint64
	incTime           uint64
	idMapper          identity.Identity
	toDieFlag         int32
	toDieChan         chan struct{}
	lock              sync.RWMutex
	aliveTimeInterval time.Duration
}

func (d *gossipDiscoveryService) periodicalSendAlive() {
	defer logging.Debug("Stopped")

	for !d.toDie() {
		logging.Debug("Sleeping", d.aliveTimeInterval)
		time.Sleep(d.aliveTimeInterval)

		msg, err := d.createSignedAliveMessage()
		if err != nil {
			logging.Warningf("Failed creating alive message: %+v", errors.WithStack(err))
			return
		}
		d.lock.Lock()
	}
}

func (d *gossipDiscoveryService) toDie() bool {
	toDie := atomic.LoadInt32(&d.toDieFlag) == int32(1)
	return toDie
}

func (d *gossipDiscoveryService) Stop() {
	defer logging.Info("Stopped")
	logging.Info("Stopping")
	atomic.StoreInt32(&d.toDieFlag, int32(1))
	d.toDieChan <- struct{}{}
}

func (d *gossipDiscoveryService) createSignedAliveMessage() (*protos.SignedRKSyncMessage, error) {
	msg := d.aliveMsg()
	signer := func(msg []byte) ([]byte, error) {
		return d.idMapper.Sign(msg)
	}

	signedMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: msg,
	}
	_, err := signedMsg.Sign(signer)
	return signedMsg, err
}

func (d *gossipDiscoveryService) aliveMsg() *protos.RKSyncMessage {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.seqNum++
	seq := d.seqNum
	endpoint := d.self.Endpoint
	pkiID := d.self.PKIID

	msg := &protos.RKSyncMessage{
		Tag: protos.RKSyncMessage_EMPTY,
		Content: &protos.RKSyncMessage_AliveMsg{
			AliveMsg: &protos.AliveMessage{
				Membership: &protos.Member{
					Endpoint: endpoint,
					PkiId:    pkiID,
				},
				Timestamp: &protos.PeerTime{
					IncNum: uint64(d.incTime),
					SeqNum: seq,
				},
			},
		},
	}

	return msg
}
