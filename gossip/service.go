/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/channel"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/discovery"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/rpc"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
)

const (
	presumedDeadChanSize = 100
	acceptChanSize       = 100
)

// NewGossipService creates a gossip instance attached to a gRPC server
func NewGossipService(gConf *config.GossipConfig, idConf *config.IdentityConfig, s *grpc.Server,
	selfIdentity common.PeerIdentityType, secureDialOpts func() []grpc.DialOption) (Gossip, error) {

	g := &gossipService{
		selfIdentity:          selfIdentity,
		conf:                  gConf,
		id:                    idConf.ID,
		presumedDead:          make(chan common.PKIidType, presumedDeadChanSize),
		toDieChan:             make(chan struct{}, 1),
		stopFlag:              int32(0),
		includeIdentityPeriod: time.Now().Add(gConf.PublishCertPeriod),
		ChannelDeMultiplexer:  rpc.NewChannelDemultiplexer(),
	}
	g.chainStateMsgStore = g.newChainStateMsgStore()

	var err error
	g.idMapper, err = identity.NewIdentity(idConf, selfIdentity, func(pkiID common.PKIidType) {
		g.srv.CloseConn(&common.NetworkMember{PKIID: pkiID})
	})
	if err != nil {
		return nil, err
	}

	g.selfPKIid = g.idMapper.GetPKIidOfCert(selfIdentity)
	g.chanState = newChannelState(g)
	g.srv = rpc.NewServer(s, g.idMapper, selfIdentity, secureDialOpts)
	g.emitter = newBatchingEmitter(gConf.PropagateIterations, gConf.MaxPropagationBurstSize,
		gConf.MaxPropagationBurstLatency, g.sendGossipBatch)

	g.discAdapter = g.newDiscoveryAdapter()
	g.disc = discovery.NewDiscoveryService(g.selfNetworkMember(), g.discAdapter, g.newDiscoverySecurityAdapter())
	logging.Infof("Creating gossip service with self membership of %s", g.selfNetworkMember())

	g.stopSignal.Add(2)
	go g.start()
	go g.connect2BootstrapPeers()

	return g, nil
}

type gossipService struct {
	id                    string
	selfIdentity          common.PeerIdentityType
	selfPKIid             common.PKIidType
	includeIdentityPeriod time.Time
	idMapper              identity.Identity
	srv                   *rpc.Server
	conf                  *config.GossipConfig
	emitter               batchingEmitter
	disc                  discovery.Discovery
	stopSignal            sync.WaitGroup
	stopFlag              int32
	toDieChan             chan struct{}
	presumedDead          chan common.PKIidType
	discAdapter           *discoveryAdapter
	chanState             *channelState
	chainStateMsgStore    lib.MessageStore
	*rpc.ChannelDeMultiplexer
}

func (g *gossipService) SelfChainInfo(chainID string) *protos.ChainState {
	ch := g.chanState.getChannelByChainID(chainID)
	if ch == nil {
		return nil
	}
	return ch.Self()
}

func (g *gossipService) SelfPKIid() common.PKIidType {
	return g.selfPKIid
}

func (g *gossipService) Peers() []common.NetworkMember {
	if g.toDie() {
		return []common.NetworkMember{}
	}
	return g.disc.GetMembership()
}

func (g *gossipService) Accept(acceptor common.MessageAcceptor, mac []byte, passThrough bool) (<-chan *protos.RKSyncMessage, <-chan protos.ReceivedMessage) {
	if passThrough {
		return nil, g.srv.Accept(acceptor)
	}
	acceptByType := func(o interface{}) bool {
		if o, isRKSyncMsg := o.(*protos.RKSyncMessage); isRKSyncMsg {
			return acceptor(o)
		}
		if o, isSignedMsg := o.(*protos.SignedRKSyncMessage); isSignedMsg {
			return acceptor(o.RKSyncMessage)
		}
		logging.Warning("Message type: ", reflect.TypeOf(o), "cannot be evaluated")
		return false
	}

	inCh := g.AddChannelWithMAC(acceptByType, mac)
	outCh := make(chan *protos.RKSyncMessage, acceptChanSize)
	go func() {
		for {
			select {
			case s := <-g.toDieChan:
				g.toDieChan <- s
				return
			case m := <-inCh:
				if m == nil {
					return
				}
				outCh <- m.(*protos.SignedRKSyncMessage).RKSyncMessage
			}
		}
	}()
	return outCh, nil
}

func (g *gossipService) InitializeChain(chainMac common.ChainMac, chainState *protos.ChainState) error {
	if len(chainMac) == 0 {
		return errors.New("Channel mac can't be nil or empty")
	}
	if g.toDie() {
		return errors.New("RKSync service is stopping")
	}
	if c := g.chanState.getChannelByMAC(chainMac); c != nil {
		return errors.Errorf("Channel (%s) already exists", chainMac)
	}

	signedMsg, err := chainState.Envelope.ToRKSyncMessage()
	if err != nil {
		return errors.Wrapf(err, "Failed to parse channel %s state information", chainMac)
	}

	err = signedMsg.Verify(g.selfPKIid, func(peerIdentity []byte, signature, message []byte) error {
		return g.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		return errors.Wrapf(err, "Failed verifying %s chain state information signature: %s", chainMac, err)
	}

	stateInfo, err := chainState.GetChainStateInfo()
	if err != nil {
		return errors.Errorf("Channel %s: state information format error: %s", chainMac, err)
	}
	if !bytes.Equal(common.PKIidType(stateInfo.Leader), g.selfPKIid) {
		return errors.Errorf("Channel %s: current peer's PKI-ID (%s) doesn't match the leader PKI-ID (%s)", chainMac, g.selfPKIid, common.PKIidType(stateInfo.Leader))
	}

	gc := g.chanState.joinChannel(chainMac, chainState.ChainId, true)
	return gc.InitializeWithChainState(chainState)
}

func (g *gossipService) AddMemberToChain(chainMac common.ChainMac, member common.PKIidType) (*protos.ChainState, error) {
	gc := g.chanState.getChannelByMAC(chainMac)
	if gc == nil {
		return nil, errors.Errorf("Channel %s not yet created", chainMac)
	}

	return gc.AddMember(member)
}

func (g *gossipService) RemoveMemberWithChain(chainMac common.ChainMac, member common.PKIidType) (*protos.ChainState, error) {
	gc := g.chanState.getChannelByMAC(chainMac)
	if gc == nil {
		return nil, errors.Errorf("Channel %s not yet created", chainMac)
	}

	return gc.RemoveMember(member)
}

func (g *gossipService) AddFileToChain(chainMac common.ChainMac, files []*common.FileSyncInfo) (*protos.ChainState, error) {
	gc := g.chanState.getChannelByMAC(chainMac)
	if gc == nil {
		return nil, errors.Errorf("Channel %s not yet created", chainMac)
	}

	return gc.AddFile(files)
}

func (g *gossipService) RemoveFileWithChain(chainMac common.ChainMac, filenames []string) (*protos.ChainState, error) {
	gc := g.chanState.getChannelByMAC(chainMac)
	if gc == nil {
		return nil, errors.Errorf("Channel %s not yet created", chainMac)
	}

	return gc.RemoveFile(filenames)
}

func (g *gossipService) GetPKIidOfCert(nodeID string, cert *x509.Certificate) (common.PKIidType, error) {
	nodeIDRaw := []byte(nodeID)
	pb := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	pemBytes := pem.EncodeToMemory(pb)
	if pemBytes == nil {
		return nil, errors.New("Encoding of certificate failed")
	}

	raw := append(nodeIDRaw, pemBytes...)
	digest := util.ComputeSHA3256(raw)
	return digest, nil
}

func (g *gossipService) CreateChain(chainMac common.ChainMac, chainID string, files []*common.FileSyncInfo) (*protos.ChainState, error) {
	if len(chainMac) == 0 {
		return nil, errors.New("Channel mac can't be nil or empty")
	}
	if chainID == "" {
		return nil, errors.New("Channel ID must be provided")
	}
	if g.toDie() {
		return nil, errors.New("RKSync service is stopping")
	}

	if c := g.chanState.getChannelByMAC(chainMac); c != nil {
		return nil, ErrChannelExist
	}

	gc := g.chanState.joinChannel(chainMac, chainID, true)
	return gc.Initialize(chainID, []common.PKIidType{g.selfPKIid}, files)
}

func (g *gossipService) CloseChain(chainMac common.ChainMac, notify bool) error {
	if len(chainMac) == 0 {
		return errors.New("Chain mac can't be nil or empty")
	}

	gc := g.chanState.getChannelByMAC(chainMac)
	if gc == nil {
		return ErrChannelNotExist
	}
	chainState := gc.Self()
	msg, err := chainState.Envelope.ToRKSyncMessage()
	if err != nil {
		return err
	}
	if !msg.IsStateInfoMsg() {
		return errors.New("Channel state message isn't well formatted")
	}
	err = msg.Verify(g.selfPKIid, func(peerIdentity []byte, signature, message []byte) error {
		return g.idMapper.Verify(peerIdentity, signature, message)
	})
	if err != nil {
		return errors.Wrap(err, "Failed verifying ChainStateInfo message")
	}

	chainInfo := msg.GetStateInfo()
	if !bytes.Equal(g.selfPKIid, common.PKIidType(chainInfo.Leader)) {
		return errors.New("Only the channel leader can close the channel")
	}
	filterFunc := gc.IsMemberInChan

	closed := g.chanState.closeChannel(chainMac)
	if notify && closed {
		peers := filter.SelectAllPeers(g.disc.GetMembership(), filterFunc)
		go g.publishLeaveChainMsg(chainMac, peers...)
	}
	return nil
}

func (g *gossipService) CreateLeaveChainMessage(chainMac common.ChainMac) (*protos.SignedRKSyncMessage, error) {
	msg := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			ChainMac: chainMac,
			Tag:      protos.RKSyncMessage_CHAN_ONLY,
			Nonce:    0,
			Content: &protos.RKSyncMessage_LeaveChain{
				LeaveChain: &protos.LeaveChainMessage{
					ChainMac: chainMac,
				},
			},
		},
	}

	_, err := msg.Sign(func(msg []byte) ([]byte, error) {
		return g.idMapper.Sign(msg)
	})

	if err != nil {
		logging.Errorf("Failed signing LeaveChainMessage: %v", err)
		return nil, err
	}

	return msg, nil
}

func (g *gossipService) Stop() {
	if g.toDie() {
		return
	}

	atomic.StoreInt32(&g.stopFlag, int32(1))
	logging.Infof("Stopping gossip instance: %s", g.id)
	defer logging.Infof("Stopped gossip instance: %s", g.id)
	g.chanState.stop()
	g.disc.Stop()
	g.discAdapter.close()
	g.toDieChan <- struct{}{}
	g.emitter.Stop()
	g.ChannelDeMultiplexer.Close()
	g.stopSignal.Wait()
	g.srv.Stop()
}

func (g *gossipService) selfNetworkMember() common.NetworkMember {
	return common.NetworkMember{
		Endpoint: g.conf.Endpoint,
		PKIID:    g.srv.GetPKIid(),
	}
}

func (g *gossipService) publishLeaveChainMsg(chainMac common.ChainMac, peers ...*common.NetworkMember) {
	msg, err := g.CreateLeaveChainMessage(chainMac)
	if err != nil {
		logging.Errorf("Failed creating LeaveChainMessage for channel %s: %s", chainMac, err)
		return
	}

	results := g.srv.SendWithAck(msg, 10*time.Second, len(peers), peers...)
	for _, res := range results {
		if res.Error() == "" {
			continue
		}
		logging.Warningf("Failed sending to %s, error: %s", res.Endpoint, res.Error())
	}

	if results.AckCount() < len(peers) {
		logging.Errorf("Publish LeaveChainMessage occurred error(s): %s", results.String())
	}
}

func (g *gossipService) sendGossipBatch(a []interface{}) {
	msgs2Gossip := make([]*emittedRKSyncMessage, len(a))
	for i, e := range a {
		msgs2Gossip[i] = e.(*emittedRKSyncMessage)
	}
	g.gossipBatch(msgs2Gossip)
}

func (g *gossipService) gossipBatch(msgs []*emittedRKSyncMessage) {
	if g.disc == nil {
		logging.Error("Discovery has not been initialized yet, aborting")
		return
	}

	var chainStateMsgs []*emittedRKSyncMessage

	isAChainStateMsg := func(o interface{}) bool {
		return o.(*emittedRKSyncMessage).IsChainStateMsg()
	}

	chainStateMsgs, msgs = partitionMessages(isAChainStateMsg, msgs)
	for _, chainStateMsg := range chainStateMsgs {
		peerSelector := func(member common.NetworkMember) bool {
			return chainStateMsg.filter(member.PKIID)
		}
		gc := g.chanState.getChannelByMAC(chainStateMsg.ChainMac)
		if gc != nil {
			peerSelector = filter.CombineRoutingFilters(peerSelector, gc.IsMemberInChan)
		}

		peers2Send := filter.SelectPeers(g.conf.PropagatePeerNum, g.disc.GetMembership(), peerSelector)
		g.srv.Send(chainStateMsg.SignedRKSyncMessage, peers2Send...)
	}

	for _, msg := range msgs {
		if !msg.IsAliveMsg() {
			logging.Error("Unknow message type", msg)
			continue
		}

		selector := filter.CombineRoutingFilters(filter.SelectAllPolicy, func(member common.NetworkMember) bool {
			return msg.filter(member.PKIID)
		})
		peers2Send := filter.SelectPeers(g.conf.PropagatePeerNum, g.disc.GetMembership(), selector)
		g.srv.Send(msg.SignedRKSyncMessage, peers2Send...)
	}
}

func (g *gossipService) start() {
	go g.syncDiscovery()
	go g.handlePresumedDead()

	msgSelector := func(msg interface{}) bool {
		gMsg, isRKSyncMsg := msg.(protos.ReceivedMessage)
		if !isRKSyncMsg {
			return false
		}

		isConn := gMsg.GetRKSyncMessage().GetConn() != nil
		isEmpty := gMsg.GetRKSyncMessage().GetEmpty() != nil

		return !(isConn || isEmpty)
	}

	incMsgs := g.srv.Accept(msgSelector)

	go g.acceptMessages(incMsgs)

	logging.Info("RKSync gossip instance", g.id, "started")
}

func (g *gossipService) acceptMessages(incMsgs <-chan protos.ReceivedMessage) {
	defer logging.Debug("Exiting")
	defer g.stopSignal.Done()
	for {
		select {
		case s := <-g.toDieChan:
			g.toDieChan <- s
			return
		case msg := <-incMsgs:
			g.handleMessage(msg)
		}
	}
}

func (g *gossipService) handleMessage(m protos.ReceivedMessage) {
	if g.toDie() {
		return
	}

	if m == nil || m.GetRKSyncMessage() == nil {
		return
	}

	msg := m.GetRKSyncMessage()

	logging.Debug("Entering,", m.GetConnectionInfo(), "sent us", msg)
	defer logging.Debug("Exiting")

	if !g.validateMsg(m) {
		logging.Warning("Message", msg, "isn't valid")
		return
	}

	if msg.IsChainStateMsg() {
		chainState := msg.GetState()
		chainInfo, err := chainState.GetChainStateInfo()
		if err != nil {
			logging.Warningf("Failed getting ChainStateInfo message: %s", err)
			return
		}

		mac := channel.GenerateMAC(chainInfo.Leader, chainState.ChainId)
		if !bytes.Equal(mac, msg.ChainMac) {
			logging.Warningf("ChainState (%s) message has an invalid MAC, expected %s, got %s, leader: %s, sent from %s",
				chainState.ChainId,
				mac,
				common.ChainMac(msg.ChainMac),
				common.PKIidType(chainInfo.Leader),
				m.GetConnectionInfo().ID)
			return
		}

		g.emitter.Add(&emittedRKSyncMessage{
			SignedRKSyncMessage: msg,
			filter:              m.GetConnectionInfo().ID.IsNotSameFilter,
		})

		added := g.chainStateMsgStore.Add(msg)
		if added {
			gc := g.chanState.lookupChannelForMsg(m)
			if gc == nil && g.isInChannel(m) {
				gc = g.chanState.joinChannel(msg.ChainMac, chainState.ChainId, false)
			}

			if gc != nil {
				gc.InitializeWithChainState(chainState)
				gc.HandleMessage(m)
			}
		}
		return
	}

	if msg.IsLeaveChain() {
		chainMac := msg.GetLeaveChain().ChainMac
		gc := g.chanState.getChannelByMAC(chainMac)
		if gc == nil {
			m.Ack(errors.Errorf("Failed getting channel %s based on leave message", chainMac))
			logging.Warningf("Failed getting channel %s based on leave message: %+v", chainMac, msg)
			return
		}

		chainState := gc.Self()
		chainInfo, err := chainState.GetChainStateInfo()
		if err != nil {
			m.Ack(errors.Errorf("Failed getting channel (%s) state information", chainMac))
			logging.Errorf("Failed getting channel (%s) state information: %s", chainMac, err)
			return
		}

		err = msg.Verify(chainInfo.Leader, func(peerIdentity []byte, signature, message []byte) error {
			return g.idMapper.Verify(peerIdentity, signature, message)
		})
		if err != nil {
			m.Ack(errors.New("Failed verifying the signature of the leave message"))
			logging.Errorf("Failed verifying the signature of the leave message: %s", err)
			return
		}

		g.chanState.closeChannel(chainMac)
		m.Ack(nil)
		return
	}

	if msg.IsChannelRestricted() {
		gc := g.chanState.lookupChannelForMsg(m)
		if gc != nil {
			gc.HandleMessage(m)
		}
		return
	}

	if selectOnlyDiscoveryMessages(m) {
		if m.GetRKSyncMessage().GetMemReq() != nil {
			sMsg, err := m.GetRKSyncMessage().GetMemReq().SelfInformation.ToRKSyncMessage()
			if err != nil {
				logging.Warningf("Got membership request with invalid selfInfo: %+v", errors.WithStack(err))
				return
			}
			if !sMsg.IsAliveMsg() {
				logging.Warning("Got membership request with selfInfo that isn't an AliveMessage")
				return
			}
			if !bytes.Equal(sMsg.GetAliveMsg().Membership.PkiId, m.GetConnectionInfo().ID) {
				logging.Warning("Got membership request with selfInfo that doesn't match the handshake")
				return
			}
		}
		g.forwardDiscoveryMsg(m)
	}
}

func (g *gossipService) isInChannel(m protos.ReceivedMessage) bool {
	msg := m.GetRKSyncMessage()
	chainStateInfo, err := msg.GetState().GetChainStateInfo()
	if err != nil {
		logging.Errorf("Failed unmarshalling ChainStateInfo message: %v", err)
		return false
	}

	for _, member := range chainStateInfo.Properties.Members {
		if bytes.Equal(member, g.selfPKIid) {
			return true
		}
	}

	return false
}

func (g *gossipService) forwardDiscoveryMsg(msg protos.ReceivedMessage) {
	if g.discAdapter.toDie() {
		return
	}

	g.discAdapter.incChan <- msg
}

func (g *gossipService) handlePresumedDead() {
	defer logging.Debug("Exiting")
	defer g.stopSignal.Done()
	for {
		select {
		case s := <-g.toDieChan:
			g.toDieChan <- s
			return
		case deadEndpoint := <-g.srv.PresumedDead():
			g.presumedDead <- deadEndpoint
		}
	}
}

// validateMsg checks the signature of the message if exists.
func (g *gossipService) validateMsg(msg protos.ReceivedMessage) bool {
	if err := msg.GetRKSyncMessage().IsTagLegal(); err != nil {
		logging.Warningf("Tag of %v isn't legal: %v", msg.GetRKSyncMessage(), errors.WithStack(err))
		return false
	}

	return true
}

func (g *gossipService) syncDiscovery() {
	logging.Debug("Entering discovery sync with interval", g.conf.PullInterval)
	defer logging.Debug("Exiting discovery sync loop")

	for !g.toDie() {
		g.disc.InitiateSync(g.conf.PullPeerNum)
		time.Sleep(g.conf.PullInterval)
	}
}

func (g *gossipService) connect2BootstrapPeers() {
	for _, endpoint := range g.conf.BootstrapPeers {
		identifier := func() (common.PKIidType, error) {
			remotePeerIdentity, err := g.srv.Handshake(&common.NetworkMember{Endpoint: endpoint})
			if err != nil {
				return nil, errors.WithStack(err)
			}
			pkiID := g.idMapper.GetPKIidOfCert(remotePeerIdentity)
			if len(pkiID) == 0 {
				return nil, errors.Errorf("Wasn't able to extract PKI-ID of remote peer with identity of %v", remotePeerIdentity)
			}
			return pkiID, nil
		}
		g.disc.Connect(common.NetworkMember{Endpoint: endpoint}, identifier)
	}
}

func (g *gossipService) toDie() bool {
	return atomic.LoadInt32(&g.stopFlag) == int32(1)
}

func (g *gossipService) newChainStateMsgStore() lib.MessageStore {
	pol := protos.NewRKSyncMessageComparator()
	return lib.NewMessageStoreExpirable(pol,
		lib.Noop,
		g.conf.PublishStateInfoInterval*100,
		nil,
		nil,
		lib.Noop)
}

func selectOnlyDiscoveryMessages(m interface{}) bool {
	msg, isRKSyncMsg := m.(protos.ReceivedMessage)
	if !isRKSyncMsg {
		return false
	}
	alive := msg.GetRKSyncMessage().GetAliveMsg()
	memRes := msg.GetRKSyncMessage().GetMemRes()
	memReq := msg.GetRKSyncMessage().GetMemReq()

	selected := alive != nil || memRes != nil || memReq != nil
	return selected
}

func (g *gossipService) newDiscoveryAdapter() *discoveryAdapter {
	return &discoveryAdapter{
		srv:      g.srv,
		stopping: int32(0),
		gossipFunc: func(msg *protos.SignedRKSyncMessage) {
			if g.conf.PropagateIterations == 0 {
				return
			}
			g.emitter.Add(&emittedRKSyncMessage{
				SignedRKSyncMessage: msg,
				filter:              func(_ common.PKIidType) bool { return true },
			})
		},
		forwardFunc: func(msg protos.ReceivedMessage) {
			if g.conf.PropagateIterations == 0 {
				return
			}
			g.emitter.Add(&emittedRKSyncMessage{
				SignedRKSyncMessage: msg.GetRKSyncMessage(),
				filter:              msg.GetConnectionInfo().ID.IsNotSameFilter,
			})
		},
		incChan:      make(chan protos.ReceivedMessage),
		presumedDead: g.presumedDead,
	}
}

// discoveryAdapter is used to supply the discovery module with needed abilities
type discoveryAdapter struct {
	stopping     int32
	srv          *rpc.Server
	presumedDead chan common.PKIidType
	incChan      chan protos.ReceivedMessage
	gossipFunc   func(message *protos.SignedRKSyncMessage)
	forwardFunc  func(message protos.ReceivedMessage)
}

func (da *discoveryAdapter) close() {
	atomic.StoreInt32(&da.stopping, int32(1))
}

func (da *discoveryAdapter) toDie() bool {
	return atomic.LoadInt32(&da.stopping) == int32(1)
}

func (da *discoveryAdapter) Gossip(msg *protos.SignedRKSyncMessage) {
	if da.toDie() {
		return
	}

	da.gossipFunc(msg)
}

func (da *discoveryAdapter) Forward(msg protos.ReceivedMessage) {
	if da.toDie() {
		return
	}

	da.forwardFunc(msg)
}

func (da *discoveryAdapter) SendToPeer(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage) {
	if da.toDie() {
		return
	}

	da.srv.Send(msg, peer)
}

func (da *discoveryAdapter) Ping(peer *common.NetworkMember) bool {
	err := da.srv.Probe(peer)
	return err == nil
}

func (da *discoveryAdapter) Accept() <-chan protos.ReceivedMessage {
	return da.incChan
}

func (da *discoveryAdapter) PresumedDead() <-chan common.PKIidType {
	return da.presumedDead
}

func (da *discoveryAdapter) CloseConn(peer *common.NetworkMember) {
	da.srv.CloseConn(peer)
}

func (g *gossipService) newDiscoverySecurityAdapter() *discoverySecurityAdapter {
	return &discoverySecurityAdapter{
		idMapper:              g.idMapper,
		includeIdentityPeriod: g.includeIdentityPeriod,
		identity:              g.selfIdentity,
	}
}

type discoverySecurityAdapter struct {
	identity              common.PeerIdentityType
	includeIdentityPeriod time.Time
	idMapper              identity.Identity
}

func (sa *discoverySecurityAdapter) ValidateAliveMsg(m *protos.SignedRKSyncMessage) bool {
	am := m.GetAliveMsg()
	if am == nil || am.Membership == nil || am.Membership.PkiId == nil || !m.IsSigned() {
		logging.Warning("Invalid alive message:", m)
		return false
	}

	if am.Identity != nil {
		identity := common.PeerIdentityType(am.Identity)
		claimedPKIID := am.Membership.PkiId
		err := sa.idMapper.Put(claimedPKIID, identity)
		if err != nil {
			logging.Debug("Falied validating identity of %v reason %+v", am, errors.WithStack(err))
			return false
		}
	} else {
		cert, _ := sa.idMapper.Get(am.Membership.PkiId)
		if cert == nil {
			logging.Debug("Don't have certificate for", am)
			return false
		}
	}

	logging.Debug("Fetched identity of", am.Membership.PkiId, "from identity store")
	return sa.validateAliveMsgSignature(m, am.Membership.PkiId)
}

func (sa *discoverySecurityAdapter) SignMessage(m *protos.RKSyncMessage) *protos.Envelope {
	signer := func(msg []byte) ([]byte, error) {
		return sa.idMapper.Sign(msg)
	}
	if m.IsAliveMsg() && time.Now().Before(sa.includeIdentityPeriod) {
		m.GetAliveMsg().Identity = sa.identity
	}

	signedMsg := &protos.SignedRKSyncMessage{RKSyncMessage: m}
	e, err := signedMsg.Sign(signer)
	if err != nil {
		logging.Warningf("Failed signing message: %+v", errors.WithStack(err))
		return nil
	}

	return e
}

func (sa *discoverySecurityAdapter) SelfIdentity() common.PeerIdentityType {
	return sa.identity
}

func (sa *discoverySecurityAdapter) validateAliveMsgSignature(m *protos.SignedRKSyncMessage, id common.PKIidType) bool {
	am := m.GetAliveMsg()
	verifier := func(pkiID []byte, signature, message []byte) error {
		return sa.idMapper.Verify(common.PKIidType(pkiID), signature, message)
	}

	err := m.Verify(id, verifier)
	if err != nil {
		logging.Warningf("Failed verifying: %v: %+v", am, errors.WithStack(err))
		return false
	}
	return true
}

// partitionMessages receives a predicate and a slice of rksync messages
// and returns a tuple of two slices: the messages that hold for the predicate
// and the rest
func partitionMessages(pred common.MessageAcceptor, a []*emittedRKSyncMessage) ([]*emittedRKSyncMessage, []*emittedRKSyncMessage) {
	s1 := []*emittedRKSyncMessage{}
	s2 := []*emittedRKSyncMessage{}
	for _, m := range a {
		if pred(m) {
			s1 = append(s1, m)
		} else {
			s2 = append(s2, m)
		}
	}
	return s1, s2
}
