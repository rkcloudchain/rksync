/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package channel

import (
	"bytes"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/channel/fsync"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
)

type gossipChannel struct {
	Adapter
	sync.RWMutex
	incTime       uint64
	seqNum        uint64
	chainID       string
	fs            config.FileSystem
	pkiID         common.PKIidType
	leader        bool
	msgStore      lib.MessageStore
	chainStateMsg *protos.ChainState
	idMapper      identity.Identity
	chainMac      common.ChainMac
	members       map[string]common.PKIidType
	fileState     *fsyncState
	stopChan      chan struct{}
}

// NewGossipChannel creates a new gossip Channel
func NewGossipChannel(pkiID common.PKIidType, chainMac common.ChainMac, chainID string, leader bool, adapter Adapter, idMapper identity.Identity) Channel {

	gc := &gossipChannel{
		incTime:  uint64(time.Now().UnixNano()),
		seqNum:   uint64(0),
		chainID:  chainID,
		pkiID:    pkiID,
		Adapter:  adapter,
		leader:   leader,
		fs:       adapter.GetChannelConfig().FileSystem,
		chainMac: chainMac,
		idMapper: idMapper,
		stopChan: make(chan struct{}, 1),
		members:  make(map[string]common.PKIidType),
	}
	gc.fileState = newFSyncState(gc)
	gc.msgStore = lib.NewMessageStoreExpirable(
		protos.NewRKSyncMessageComparator(),
		lib.Noop,
		gc.GetChannelConfig().RequestStateInfoInterval*100,
		nil,
		nil,
		lib.Noop)

	if gc.leader {
		go gc.periodicalPublishStateInfo(adapter.GetChannelConfig().PublishStateInfoInterval)
	} else {
		go gc.periodicalRequestStateInfo(adapter.GetChannelConfig().RequestStateInfoInterval)
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
		gc.members[common.PKIidType(member).String()] = member
	}

	for _, file := range stateInfo.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, file.Metadata, gc.leader)
		if err != nil {
			return err
		}
	}

	gc.chainStateMsg = chainState
	return nil
}

func (gc *gossipChannel) Initialize(chainID string, members []common.PKIidType, files []*common.FileSyncInfo) (*protos.ChainState, error) {
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
		gc.members[member.String()] = member
		stateInfo.Properties.Members[i] = []byte(member)
	}
	for i, file := range files {
		mode, ok := protos.File_Mode_value[file.Mode]
		if !ok {
			return nil, errors.Errorf("Unknown file mode %s", file.Mode)
		}

		stateInfo.Properties.Files[i] = &protos.File{
			Path:     file.Path,
			Mode:     protos.File_Mode(mode),
			Metadata: file.Metadata,
		}
	}

	stateInfoMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Tag:      protos.RKSyncMessage_CHAN_ONLY,
			ChainMac: gc.chainMac,
			Nonce:    0,
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
		ChainId:  chainID,
		Envelope: envp,
	}
	gc.chainStateMsg = chainState

	for _, file := range stateInfo.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, file.Metadata, gc.leader)
		if err != nil {
			return nil, errors.Wrap(err, "Failed creating file sync provider")
		}
	}

	return chainState, nil
}

func (gc *gossipChannel) AddMember(member common.PKIidType) (*protos.ChainState, error) {
	// can't add self to the members
	if bytes.Equal(member, gc.pkiID) {
		return nil, errors.New("Can't add self-node to the channel members")
	}

	gc.Lock()
	defer gc.Unlock()

	msg, stateInfo, err := gc.validateChainLeader()
	if err != nil {
		return nil, err
	}

	for _, m := range gc.members {
		if bytes.Equal(member, common.PKIidType(m)) {
			return gc.chainStateMsg, nil
		}
	}

	stateInfo.Properties.Members = append(stateInfo.Properties.Members, member)
	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	gc.chainStateMsg.Envelope = envp
	gc.chainStateMsg.SeqNum = uint64(time.Now().UnixNano())
	gc.members[member.String()] = member

	return gc.chainStateMsg, nil
}

func (gc *gossipChannel) RemoveMember(member common.PKIidType) (*protos.ChainState, error) {
	gc.Lock()
	defer gc.Unlock()

	msg, stateInfo, err := gc.validateChainLeader()
	if err != nil {
		return nil, err
	}
	if bytes.Equal(member, common.PKIidType(stateInfo.Leader)) {
		return nil, errors.New("Can't remove youself out of the channel")
	}

	var found bool
	n := len(stateInfo.Properties.Members)
	for i := 0; i < n; i++ {
		m := stateInfo.Properties.Members[i]
		if bytes.Equal(member, common.PKIidType(m)) {
			stateInfo.Properties.Members = append(stateInfo.Properties.Members[:i], stateInfo.Properties.Members[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return gc.chainStateMsg, nil
	}

	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	gc.chainStateMsg.Envelope = envp
	gc.chainStateMsg.SeqNum = uint64(time.Now().UnixNano())
	delete(gc.members, member.String())

	filterFunc := func(nm common.NetworkMember) bool {
		return bytes.Equal(nm.PKIID, member)
	}
	peers := filter.SelectAllPeers(gc.GetMembership(), filterFunc)
	if len(peers) > 0 {
		go gc.sendLeaveChainMessage(peers[0])
	}

	return gc.chainStateMsg, nil
}

func (gc *gossipChannel) AddFile(files []*common.FileSyncInfo) (*protos.ChainState, error) {
	gc.Lock()
	defer gc.Unlock()

	msg, stateInfo, err := gc.validateChainLeader()
	if err != nil {
		return nil, err
	}

	var fnames []string
	for _, file := range files {
		if gc.fileState.lookupFSyncProviderByFilename(file.Path) != nil {
			logging.Warningf("File %s has already exists", file.Path)
			continue
		}

		mode, exists := protos.File_Mode_value[file.Mode]
		if !exists {
			err = errors.Errorf("Unknow file mode: %s", file.Mode)
			break
		}

		f := &protos.File{Path: file.Path, Mode: protos.File_Mode(mode), Metadata: file.Metadata}
		stateInfo.Properties.Files = append(stateInfo.Properties.Files, f)

		err = gc.fileState.createProvider(file.Path, protos.File_Mode(mode), file.Metadata, gc.leader)
		if err != nil {
			break
		}
		fnames = append(fnames, file.Path)
	}

	if err != nil {
		gc.closeFSyncer(fnames)
		return nil, err
	}

	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		gc.closeFSyncer(fnames)
		return nil, err
	}

	gc.chainStateMsg.Envelope = envp
	gc.chainStateMsg.SeqNum = uint64(time.Now().UnixNano())
	return gc.chainStateMsg, nil
}

func (gc *gossipChannel) RemoveFile(filenames []string) (*protos.ChainState, error) {
	gc.Lock()
	defer gc.Unlock()

	msg, stateInfo, err := gc.validateChainLeader()
	if err != nil {
		return nil, err
	}

	var fnames []string
	for _, filename := range filenames {
		n := len(stateInfo.Properties.Files)
		for i := 0; i < n; i++ {
			f := stateInfo.Properties.Files[i]
			if f.Path == filename {
				stateInfo.Properties.Files = append(stateInfo.Properties.Files[:i], stateInfo.Properties.Files[i+1:]...)
				fnames = append(fnames, f.Path)
				break
			}
		}
	}

	envp, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return gc.idMapper.Sign(msg)
	})
	if err != nil {
		return nil, err
	}

	gc.closeFSyncer(fnames)
	gc.chainStateMsg.Envelope = envp
	gc.chainStateMsg.SeqNum = uint64(time.Now().UnixNano())

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
		member := common.NetworkMember{Endpoint: msg.GetConnectionInfo().Endpoint, PKIID: msg.GetConnectionInfo().ID}
		if !gc.IsMemberInChan(member) {
			go gc.sendLeaveChainMessage(&member)
		} else if gc.msgStore.Add(m) {
			resp, err := gc.createChainStateResponse()
			if err != nil {
				logging.Errorf("Failed creating ChainStateResponse message: %v", err)
				return
			}
			msg.Respond(resp)
		}
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
			logging.Warningf("Channel %s: Failed validating ChainState message: %v", gc.chainMac, err)
			return
		}

		err = gc.updateChainState(m.GetState(), msg.GetConnectionInfo().ID)
		if err == nil {
			gc.Forward(msg)
		} else {
			logging.Errorf("Failed updating chain state message: %s", err)
		}
	}

	if m.IsDataMsg() || m.IsDataReq() {
		if m.IsDataReq() {
			if !gc.IsMemberInChan(common.NetworkMember{PKIID: msg.GetConnectionInfo().ID}) {
				logging.Warningf("Received Data request message from %s, not member in channel %s", msg.GetConnectionInfo().ID, gc.chainMac)
				return
			}
		}

		if m.IsDataMsg() {
			if gc.leader {
				logging.Infof("Channel %s: Leader does not need to handle data message", gc.chainMac)
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
	gc.msgStore.Stop()
	gc.fileState.stop()
}

func (gc *gossipChannel) handleChainStateResponse(m *protos.RKSyncMessage, sender common.PKIidType) {
	envelope := m.GetStatePullResponse().Element
	chainState, err := envelope.ToRKSyncMessage()
	if err != nil {
		logging.Warningf("Channel %s: ChainState contains an invalid message: %+v", gc.chainMac, err)
		return
	}

	if !chainState.IsChainStateMsg() {
		logging.Warningf("Channel %s: Element of ChainStateResponse isn't a ChainState: %s, message sent from %s", gc.chainMac, chainState, sender)
		return
	}

	cs := chainState.GetState()
	if !bytes.Equal(m.ChainMac, gc.chainMac) {
		logging.Warningf("Channel %s: ChainState message has an invalid MAC, expected %s, got %s, sent from %s", gc.chainMac, gc.chainMac, m.ChainMac, sender)
		return
	}

	err = chainState.Verify(sender, func(peerIdentity []byte, signature, message []byte) error {
		return gc.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		logging.Warningf("Channel %s: Failed validating ChainState message: %v, sent from: %s", gc.chainMac, err, sender)
		return
	}

	gc.updateChainState(cs, sender)
}

func (gc *gossipChannel) updateChainState(msg *protos.ChainState, sender common.PKIidType) error {
	if gc.leader {
		logging.Infof("Channel %s: Leader does not need to update chain state", gc.chainMac)
		return nil
	}
	chainStateInfo, err := msg.Envelope.ToRKSyncMessage()
	if err != nil {
		logging.Warningf("Channel %s: ChainState's envelope contains an invalid message: %+v", gc.chainMac, err)
		return err
	}

	if !chainStateInfo.IsStateInfoMsg() {
		logging.Warningf("Channel %s: Element of ChainState isn't a ChainStateInfo: %s, message sent from %s", gc.chainMac, chainStateInfo, sender)
		return errors.New("Element of ChainState isn't a ChainStateInfo")
	}

	csi := chainStateInfo.GetStateInfo()
	err = chainStateInfo.Verify(csi.Leader, func(peerIdentity []byte, signature, message []byte) error {
		return gc.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		logging.Warningf("Channel %s: Failed validating ChainStateInfo message: %v, sent from: %s", gc.chainMac, err, sender)
		return err
	}

	gc.Lock()
	defer gc.Unlock()

	gc.chainStateMsg = msg
	gc.members = make(map[string]common.PKIidType)
	for _, member := range csi.Properties.Members {
		gc.members[common.PKIidType(member).String()] = member
	}

	fnames := gc.fileState.snapshot()
	for _, fname := range fnames {
		has := contains(csi.Properties.Files, fname)
		if !has {
			gc.fileState.closeFSyncProvider(fname)
			gc.Unregister(fsync.GenerateMAC(gc.chainMac, fname))
		}
	}
	for _, file := range csi.Properties.Files {
		err := gc.fileState.createProvider(file.Path, file.Mode, file.Metadata, gc.leader)
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
			ChainMac: gc.chainMac,
			Tag:      protos.RKSyncMessage_CHAN_ONLY,
			Nonce:    0,
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
		ChainMac: gc.chainMac,
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Nonce:    0,
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

	if !bytes.Equal(gc.chainMac, m.ChainMac) {
		logging.Warning("Message contains wrong channel MAC (", m.ChainMac, "), expected", gc.chainMac)
		return false
	}

	return true
}

func (gc *gossipChannel) periodicalPublishStateInfo(dur time.Duration) {
	for {
		select {
		case <-time.After(dur):
			gc.publishStateInfo()
		case s := <-gc.stopChan:
			gc.stopChan <- s
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
			ChainMac: gc.chainMac,
			Tag:      protos.RKSyncMessage_CHAN_ONLY,
			Nonce:    0,
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

func (gc *gossipChannel) periodicalRequestStateInfo(dur time.Duration) {
	for {
		select {
		case <-time.After(dur):
			gc.requestStateInfo()
		case s := <-gc.stopChan:
			gc.stopChan <- s
			return
		}
	}
}

func (gc *gossipChannel) requestStateInfo() {
	req, err := gc.createStateInfoRequest()
	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", err)
		return
	}

	filters := filter.CombineRoutingFilters(gc.IsMemberInChan, func(member common.NetworkMember) bool {
		return gc.pkiID.IsNotSameFilter(member.PKIID)
	})
	endpoints := filter.SelectPeers(gc.GetChannelConfig().PullPeerNum, gc.GetMembership(), filters)
	gc.Send(req, endpoints...)
}

func (gc *gossipChannel) createStateInfoRequest() (*protos.SignedRKSyncMessage, error) {
	gc.Lock()
	defer gc.Unlock()
	gc.seqNum++
	seq := gc.seqNum

	return (&protos.RKSyncMessage{
		Tag:      protos.RKSyncMessage_CHAN_ONLY,
		Nonce:    0,
		ChainMac: gc.chainMac,
		Content: &protos.RKSyncMessage_StatePullRequest{
			StatePullRequest: &protos.ChainStatePullRequest{
				Timestamp: &protos.PeerTime{
					IncNum: gc.incTime,
					SeqNum: seq,
				},
			},
		},
	}).NoopSign()
}

func (gc *gossipChannel) sendLeaveChainMessage(member *common.NetworkMember) {
	msg, err := gc.CreateLeaveChainMessage(gc.chainMac)
	if err != nil {
		logging.Errorf("Failed creating LeaveChainMessage: %s", err)
		return
	}

	err = gc.SendWithAck(msg, time.Second*5, 1, member)
	if err != nil {
		logging.Errorf("Failed sending LeaveChainMessage to %s: %s", member, err)
	}
}

func (gc *gossipChannel) validateChainLeader() (*protos.SignedRKSyncMessage, *protos.ChainStateInfo, error) {
	msg, err := gc.chainStateMsg.Envelope.ToRKSyncMessage()
	if err != nil {
		return nil, nil, err
	}

	if !msg.IsStateInfoMsg() {
		return nil, nil, errors.New("Channel state message isn't well formatted")
	}
	err = msg.Verify(gc.pkiID, func(peerIdentity []byte, signature, message []byte) error {
		return gc.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed verifying ChainStateInfo message")
	}

	stateInfo := msg.GetStateInfo()
	if !bytes.Equal(gc.pkiID, stateInfo.Leader) {
		return nil, nil, errors.New("Only the channel leader can modify the channel state")
	}

	return msg, stateInfo, nil
}

func (gc *gossipChannel) closeFSyncer(fnames []string) {
	if len(fnames) == 0 {
		return
	}

	for _, fname := range fnames {
		gc.fileState.closeFSyncProvider(fname)
		gc.Unregister(fsync.GenerateMAC(gc.chainMac, fname))
	}
}
