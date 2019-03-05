package channel

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

type gossipChannel struct {
	Adapter
	sync.RWMutex
	shouldGossipStateInfo     int32
	pkiID                     common.PKIidType
	leader                    bool
	chainStateMsg             *protos.ChainState
	idMapper                  identity.Identity
	chainID                   string
	members                   []common.PKIidType
	stateInfoPublishScheduler *time.Ticker
	stateInfoRequestScheduler *time.Ticker
	incTime                   uint64
	stopChan                  chan struct{}
}

// NewGossipChannel creates a new gossip Channel
func NewGossipChannel(pkiID common.PKIidType, chainID string, leader bool, adapter Adapter, idMapper identity.Identity) Channel {

	gc := &gossipChannel{
		incTime:                   uint64(time.Now().UnixNano()),
		pkiID:                     pkiID,
		Adapter:                   adapter,
		leader:                    leader,
		chainID:                   chainID,
		shouldGossipStateInfo:     int32(0),
		idMapper:                  idMapper,
		stopChan:                  make(chan struct{}),
		stateInfoPublishScheduler: time.NewTicker(adapter.GetChannelConfig().PublishStateInfoInterval),
		stateInfoRequestScheduler: time.NewTicker(adapter.GetChannelConfig().RequestStateInfoInterval),
		members:                   []common.PKIidType{},
	}

	go gc.periodicalInvocation(gc.publishStateInfo, gc.stateInfoPublishScheduler.C)
	if !gc.leader {
		go gc.periodicalInvocation(gc.requestStateInfo, gc.stateInfoRequestScheduler.C)
	}

	return gc
}

func (gc *gossipChannel) UpdateMembers(members []common.PKIidType) {

}

func (gc *gossipChannel) UpdateFiles(files []*common.FileSyncInfo) {

}

func (gc *gossipChannel) HandleMessage(msg protos.ReceivedMessage) {
	if !gc.verifyMsg(msg) {
		logging.Warning("Failed verifying message:", msg.GetRKSyncMessage().RKSyncMessage)
		return
	}

	m := msg.GetRKSyncMessage()
	if !m.IsChannelRestricted() {
		logging.Warning("Got message", msg.GetRKSyncMessage(), "but it's not a per-channel message, discarding it")
		return
	}

	if m.IsStatePullRequestMsg() {
		resp, err := gc.createChainStateResponse()
		if err != nil {
			logging.Errorf("Failed creating ChainStateResponse message: %v", err)
			return
		}
		msg.Respond(resp)
		return
	}

	if m.IsStatePullResponseMsg() {
		gc.handleChainStateResponse(m.RKSyncMessage, msg.GetConnectionInfo().ID)
		return
	}

	if m.IsChainStateMsg() {
		err := m.Verify(msg.GetConnectionInfo().ID, func(peerIdentity []byte, signature, message []byte) error {
			return gc.idMapper.Verify(peerIdentity, signature, message)
		})
		if err != nil {
			logging.Warningf("Channel %s: Failed validating ChainState message: %v", gc.chainID, err)
			return
		}

		err = gc.updateChainState(m.GetState(), msg.GetConnectionInfo().ID)
		if err == nil {
			gc.Forward(msg)
		}
	}
}

func (gc *gossipChannel) IsMemberInChan(member common.NetworkMember) bool {
	gc.RLock()
	defer gc.RUnlock()
	for _, m := range gc.members {
		if bytes.Equal(member.PKIID, m) {
			return true
		}
	}

	return false
}

func (gc *gossipChannel) Stop() {
	gc.stopChan <- struct{}{}
	gc.stateInfoPublishScheduler.Stop()
	gc.stateInfoRequestScheduler.Stop()
}

func (gc *gossipChannel) handleChainStateResponse(m *protos.RKSyncMessage, sender common.PKIidType) {
	envelope := m.GetStatePullResponse().Element
	chainState, err := envelope.ToRKSyncMessage()
	if err != nil {
		logging.Warningf("Channel %s: ChainState contains an invalid message: %+v", gc.chainID, err)
		return
	}

	if !chainState.IsChainStateMsg() {
		logging.Warningf("Channel %s: Element of ChainStateResponse isn't a ChainState: %s, message sent from %s", gc.chainID, chainState, sender)
		return
	}

	cs := chainState.GetState()
	expectedMAC := GenerateMAC(sender, gc.chainID)
	if !bytes.Equal(cs.ChainMac, expectedMAC) {
		logging.Warningf("Channel %s: ChainState message has an invalid MAC, expected %s, got %s, sent from %s", gc.chainID, expectedMAC, cs.ChainMac, sender)
		return
	}

	err = chainState.Verify(sender, func(peerIdentity []byte, signature, message []byte) error {
		return gc.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		logging.Warningf("Channel %s: Failed validating ChainState message: %v, sent from: %s", gc.chainID, err, sender)
		return
	}

	gc.updateChainState(cs, sender)
}

func (gc *gossipChannel) updateChainState(msg *protos.ChainState, sender common.PKIidType) error {
	chainStateInfo, err := msg.Envelope.ToRKSyncMessage()
	if err != nil {
		logging.Warningf("Channel %s: ChainState's envelope contains an invalid message: %+v", gc.chainID, err)
		return err
	}

	if !chainStateInfo.IsStateInfoMsg() {
		logging.Warningf("Channel %s: Element of ChainState isn't a ChainStateInfo: %s, message sent from %s", gc.chainID, chainStateInfo, sender)
		return errors.New("Element of ChainState isn't a ChainStateInfo")
	}

	csi := chainStateInfo.GetStateInfo()
	err = chainStateInfo.Verify(csi.Leader, func(peerIdentity []byte, signature, message []byte) error {
		return gc.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		logging.Warningf("Channel %s: Failed validating ChainStateInfo message: %v, sent from: %s", gc.chainID, err, sender)
		return err
	}

	gc.Lock()
	defer gc.Unlock()

	gc.chainStateMsg = msg
	gc.members = make([]common.PKIidType, len(csi.Properties.Members))
	for index, member := range csi.Properties.Members {
		gc.members[index] = common.PKIidType(member)
	}

	return nil
}

func (gc *gossipChannel) createChainStateResponse() (*protos.RKSyncMessage, error) {
	gc.RLock()
	defer gc.RUnlock()
	element := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Channel: []byte(gc.chainID),
			Tag:     protos.RKSyncMessage_CHAN_ONLY,
			Nonce:   0,
			Content: &protos.RKSyncMessage_State{
				State: gc.chainStateMsg,
			},
		},
	}

	_, err := element.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	return &protos.RKSyncMessage{
		Channel: []byte(gc.chainID),
		Tag:     protos.RKSyncMessage_CHAN_ONLY,
		Nonce:   0,
		Content: &protos.RKSyncMessage_StatePullResponse{
			StatePullResponse: &protos.ChainStatePullResponse{
				Element: element.Envelope,
			},
		},
	}, nil
}

func (gc *gossipChannel) verifyMsg(msg protos.ReceivedMessage) bool {
	if msg == nil {
		logging.Warning("Message is nil")
		return false
	}

	m := msg.GetRKSyncMessage()
	if m == nil {
		logging.Warning("Message content is empty")
		return false
	}

	if msg.GetConnectionInfo().ID == nil {
		logging.Warning("Message has nil PKI-ID")
		return false
	}

	if m.IsChainStateMsg() {
		si := m.GetState()
		expectedMAC := GenerateMAC(msg.GetConnectionInfo().ID, gc.chainID)
		if !bytes.Equal(expectedMAC, si.ChainMac) {
			logging.Warning("Message contains wrong channel MAC (", si.ChainMac, "), expected", expectedMAC)
			return false
		}
		return true
	}

	if m.IsStatePullRequestMsg() {
		sipr := m.GetStatePullRequest()
		expectedMAC := GenerateMAC(msg.GetConnectionInfo().ID, gc.chainID)
		if !bytes.Equal(expectedMAC, sipr.ChainMac) {
			logging.Warning("Message contains wrong channel MAC (", sipr.ChainMac, "), expected", expectedMAC)
			return false
		}
		return true
	}

	if !bytes.Equal(m.Channel, []byte(gc.chainID)) {
		logging.Warning("Message contains wrong channel (", string(m.Channel), "), exptected", gc.chainID)
		return false
	}

	return true
}

func (gc *gossipChannel) periodicalInvocation(fn func(), c <-chan time.Time) {
	for {
		select {
		case <-c:
			fn()
		case <-gc.stopChan:
			gc.stopChan <- struct{}{}
			return
		}
	}
}

func (gc *gossipChannel) publishStateInfo() {
	if atomic.LoadInt32(&gc.shouldGossipStateInfo) == int32(0) {
		return
	}

	gc.RLock()
	chainStateMsg := gc.chainStateMsg
	gc.RUnlock()

	msg := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Channel: []byte(gc.chainID),
			Tag:     protos.RKSyncMessage_CHAN_ONLY,
			Nonce:   0,
			Content: &protos.RKSyncMessage_State{
				State: chainStateMsg,
			},
		},
	}

	_, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})

	if err != nil {
		logging.Errorf("Failed signing ChainState message: %v", err)
		return
	}

	gc.Gossip(msg)

	if len(gc.GetMembership()) > 0 {
		atomic.StoreInt32(&gc.shouldGossipStateInfo, int32(0))
	}
}

func (gc *gossipChannel) requestStateInfo() {
	req, err := gc.createStateInfoRequest()
	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", err)
		return
	}

	endpoints := filter.SelectPeers(gc.GetChannelConfig().PullPeerNum, gc.GetMembership(), gc.IsMemberInChan)
	gc.Send(req, endpoints...)
}

func (gc *gossipChannel) createStateInfoRequest() (*protos.SignedRKSyncMessage, error) {
	return (&protos.RKSyncMessage{
		Tag:   protos.RKSyncMessage_CHAN_ONLY,
		Nonce: 0,
		Content: &protos.RKSyncMessage_StatePullRequest{
			StatePullRequest: &protos.ChainStatePullRequest{
				ChainMac: GenerateMAC(gc.pkiID, gc.chainID),
			},
		},
	}).NoopSign()
}
