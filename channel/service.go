package channel

import (
	"bytes"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

type gossipChannel struct {
	Adapter
	sync.RWMutex
	fs                        config.FileSystem
	pkiID                     common.PKIidType
	leader                    bool
	chainStateMsg             *protos.ChainState
	idMapper                  identity.Identity
	chainID                   string
	members                   map[string]common.PKIidType
	fileState                 *fsyncState
	stateInfoPublishScheduler *time.Ticker
	stateInfoRequestScheduler *time.Ticker
	stopChan                  chan struct{}
}

// NewGossipChannel creates a new gossip Channel
func NewGossipChannel(pkiID common.PKIidType, chainID string, leader bool, adapter Adapter, idMapper identity.Identity) Channel {

	gc := &gossipChannel{
		pkiID:                     pkiID,
		Adapter:                   adapter,
		leader:                    leader,
		fs:                        adapter.GetChannelConfig().FileSystem,
		chainID:                   chainID,
		idMapper:                  idMapper,
		stopChan:                  make(chan struct{}),
		stateInfoPublishScheduler: time.NewTicker(adapter.GetChannelConfig().PublishStateInfoInterval),
		stateInfoRequestScheduler: time.NewTicker(adapter.GetChannelConfig().RequestStateInfoInterval),
		members:                   make(map[string]common.PKIidType),
	}
	gc.fileState = newFSyncState(gc)

	go gc.periodicalInvocation(gc.publishStateInfo, gc.stateInfoPublishScheduler.C)
	if !gc.leader {
		go gc.periodicalInvocation(gc.requestStateInfo, gc.stateInfoRequestScheduler.C)
	}

	return gc
}

func (gc *gossipChannel) Self() *protos.ChainState {
	gc.RLock()
	defer gc.RUnlock()
	return gc.chainStateMsg
}

func (gc *gossipChannel) InitializeWithChainState(chainState *protos.ChainState) error {
	gc.Lock()
	defer gc.Unlock()

	stateInfo, err := chainState.GetChainStateInfo()
	if err != nil {
		return err
	}

	for _, member := range stateInfo.Properties.Members {
		gc.members[string(common.PKIidType(member))] = member
	}

	for _, file := range stateInfo.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, true)
		if err != nil {
			return err
		}
	}

	gc.chainStateMsg = chainState
	return nil
}

func (gc *gossipChannel) Initialize(members []common.PKIidType, files []common.FileSyncInfo) (*protos.ChainState, error) {
	gc.Lock()
	defer gc.Unlock()

	stateInfo := &protos.ChainStateInfo{
		Leader: gc.pkiID,
		Properties: &protos.Properties{
			Members: make([][]byte, len(members)),
			Files:   make([]*protos.File, len(files)),
		},
	}

	for i, member := range members {
		gc.members[string(common.PKIidType(member))] = member
		stateInfo.Properties.Members[i] = []byte(member)
	}
	for i, file := range files {
		mode, ok := protos.File_Mode_value[file.Mode]
		if !ok {
			return nil, errors.Errorf("Unknown file mode %s", file.Mode)
		}

		stateInfo.Properties.Files[i] = &protos.File{
			Path: file.Path,
			Mode: protos.File_Mode(mode),
		}
	}

	stateInfoMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Tag:     protos.RKSyncMessage_CHAN_ONLY,
			Channel: []byte(gc.chainID),
			Nonce:   0,
			Content: &protos.RKSyncMessage_StateInfo{
				StateInfo: stateInfo,
			},
		},
	}

	envp, err := stateInfoMsg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	chainState := &protos.ChainState{
		SeqNum:   uint64(time.Now().UnixNano()),
		ChainMac: GenerateMAC(gc.pkiID, gc.chainID),
		Envelope: envp,
	}
	gc.chainStateMsg = chainState

	for _, file := range stateInfo.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, true)
		if err != nil {
			return nil, errors.Wrap(err, "Failed creating file sync provider")
		}
	}

	return chainState, nil
}

func (gc *gossipChannel) AddMember(member common.PKIidType) (*protos.ChainState, error) {
	gc.Lock()
	defer gc.Unlock()

	msg, err := gc.chainStateMsg.Envelope.ToRKSyncMessage()
	if err != nil {
		return nil, err
	}

	if !msg.IsStateInfoMsg() {
		return nil, errors.New("Channel state message isn't well formatted")
	}

	stateInfo := msg.GetStateInfo()
	if !bytes.Equal(gc.pkiID, stateInfo.Leader) {
		return nil, errors.New("Only the channel leader can modify the channel state")
	}

	stateInfo.Properties.Members = append(stateInfo.Properties.Members, member)
	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	gc.chainStateMsg.Envelope = envp
	gc.members[string(member)] = member
	return gc.chainStateMsg, nil
}

func (gc *gossipChannel) AddFile(file common.FileSyncInfo) (*protos.ChainState, error) {
	if gc.fileState.lookupFSyncProviderByFilename(file.Path) != nil {
		return nil, errors.Errorf("File %s has already exists ", file.Path)
	}

	gc.Lock()
	defer gc.Unlock()

	msg, err := gc.chainStateMsg.Envelope.ToRKSyncMessage()
	if err != nil {
		return nil, err
	}

	if !msg.IsStateInfoMsg() {
		return nil, errors.New("Channel state message isn't well formatted")
	}

	stateInfo := msg.GetStateInfo()
	if !bytes.Equal(gc.pkiID, stateInfo.Leader) {
		return nil, errors.New("Only the channel leader can modify the channel state")
	}

	mode, exists := protos.File_Mode_value[file.Mode]
	if !exists {
		return nil, errors.Errorf("Unknow file mode: %s", file.Mode)
	}
	f := &protos.File{Path: file.Path, Mode: protos.File_Mode(mode)}
	stateInfo.Properties.Files = append(stateInfo.Properties.Files, f)

	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}
	gc.chainStateMsg.Envelope = envp

	gc.fileState.createProvider(file.Path, protos.File_Mode(mode), true)
	return gc.chainStateMsg, nil
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

	if m.IsDataMsg() || m.IsDataReq() {
		if m.IsDataMsg() {
			if gc.leader {
				logging.Infof("Channel %s: Leader does not need to handle data message", gc.chainID)
				return
			}

			if m.GetDataMsg().Payload == nil {
				logging.Warningf("Payload is empty, got it from %s", msg.GetConnectionInfo().ID)
				return
			}
		}

		verifier := func(peerIdentity []byte, signature, message []byte) error {
			return gc.idMapper.Verify(peerIdentity, signature, message)
		}
		err := m.Verify(msg.GetConnectionInfo().ID, verifier)
		if err != nil {
			logging.Errorf("Failed verifying message signature: %s, got it from %s", err, msg.GetConnectionInfo().ID)
			return
		}

		gc.DeMultiplex(m)
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
	gc.fileState.stop()
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
	if gc.leader {
		logging.Infof("Channel %s: Leader does not need to update chain state", gc.chainID)
		return nil
	}
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

	if gc.chainStateMsg != nil {
		oldMsg, err := gc.chainStateMsg.Envelope.ToRKSyncMessage()
		if err != nil {
			logging.Errorf("Failed unmarshalling channel state message: %s", err)
			return err
		}

		if !bytes.Equal(oldMsg.GetStateInfo().Leader, csi.Leader) {
			logging.Warningf("Channel %s: Leader has been changed, original %s, now is %s", gc.chainID, oldMsg.GetStateInfo().Leader, csi.Leader)
			return errors.New("Channel's leader has been changed")
		}
	}

	gc.chainStateMsg = msg
	gc.members = make(map[string]common.PKIidType)
	for _, member := range csi.Properties.Members {
		gc.members[string(member)] = member
	}

	for _, file := range csi.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, false)
		if err != nil {
			return errors.Wrapf(err, "Failed creating file sync provider for %s", file.Path)
		}
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
