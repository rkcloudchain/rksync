/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

const (
	maxConnectionAttempts = 120
	msgExpirationFactor   = 20
	defaultHelloInterval  = time.Duration(5) * time.Second
)

type timestamp struct {
	incTime  time.Time
	seqNum   uint64
	lastSeen time.Time
}

func (ts *timestamp) String() string {
	return fmt.Sprintf("%v, %v", ts.incTime.UnixNano(), ts.seqNum)
}

// NewDiscoveryService returns a new discovery service
func NewDiscoveryService(self common.NetworkMember, rpc RPCService, crypt CryptoService, disPol DisclosurePolicy) Discovery {
	d := &gossipDiscoveryService{
		self:                         self,
		incTime:                      uint64(time.Now().UnixNano()),
		seqNum:                       uint64(0),
		deadLastTS:                   make(map[string]*timestamp),
		aliveLastTS:                  make(map[string]*timestamp),
		id2Member:                    make(map[string]*common.NetworkMember),
		aliveMembership:              lib.NewMembershipStore(),
		deadMembership:               lib.NewMembershipStore(),
		crypt:                        crypt,
		rpc:                          rpc,
		toDieChan:                    make(chan struct{}, 1),
		toDieFlag:                    int32(0),
		disclosurePolicy:             disPol,
		pubsub:                       lib.NewPubSub(),
		aliveTimeInterval:            defaultHelloInterval,
		aliveExpirationTimeout:       5 * defaultHelloInterval,
		aliveExpirationCheckInterval: 5 * defaultHelloInterval / 10,
		reconnectInterval:            5 * defaultHelloInterval,
	}

	d.msgStore = newAliveMsgStore(d)

	go d.periodicalSendAlive()
	go d.periodicalCheckAlive()
	go d.handleMessage()
	go d.periodicalReconnectToDead()
	go d.handlePresumedDeadPeers()

	return d
}

type gossipDiscoveryService struct {
	incTime                      uint64
	seqNum                       uint64
	self                         common.NetworkMember
	deadLastTS                   map[string]*timestamp
	aliveLastTS                  map[string]*timestamp
	id2Member                    map[string]*common.NetworkMember
	rpc                          RPCService
	crypt                        CryptoService
	lock                         sync.RWMutex
	pubsub                       *lib.PubSub
	aliveMembership              *lib.MembershipStore
	deadMembership               *lib.MembershipStore
	selfAliveMessage             *protos.SignedRKSyncMessage
	msgStore                     *aliveMsgStore
	toDieChan                    chan struct{}
	toDieFlag                    int32
	disclosurePolicy             DisclosurePolicy
	reconnectInterval            time.Duration
	aliveTimeInterval            time.Duration
	aliveExpirationTimeout       time.Duration
	aliveExpirationCheckInterval time.Duration
}

func (d *gossipDiscoveryService) Connect(member common.NetworkMember, id identifier) {
	if d.isMyOwnEndpoint(member.Endpoint) {
		logging.Debug("Skipping connecting to myself")
		return
	}

	logging.Debug("Entering", member)
	defer logging.Debug("Exiting")
	go func() {
		for i := 0; i < maxConnectionAttempts && !d.toDie(); i++ {
			id, err := id()
			if err != nil {
				if d.toDie() {
					return
				}
				logging.Warningf("Could not connect to %v: %v", member, err)
				time.Sleep(d.reconnectInterval)
				continue
			}
			peer := &common.NetworkMember{Endpoint: member.Endpoint, PKIID: id}
			m, err := d.createMembershipRequest()
			if err != nil {
				logging.Warningf("Failed creating membership request: %+v", errors.WithStack(err))
				continue
			}
			req, err := m.NoopSign()
			if err != nil {
				logging.Warningf("Failed creating SignedRKSyncMessage: %+v", errors.WithStack(err))
				continue
			}
			req.Nonce = util.RandomUInt64()
			req, err = req.NoopSign()
			if err != nil {
				logging.Warningf("Failed adding NONCE to SignedRKSyncMessage: %+v", errors.WithStack(err))
				continue
			}
			go d.sendUntilAcked(peer, req)
			return
		}
	}()
}

func (d *gossipDiscoveryService) Lookup(pkiID common.PKIidType) *common.NetworkMember {
	if bytes.Equal(pkiID, d.self.PKIID) {
		return &d.self
	}
	d.lock.RLock()
	defer d.lock.RUnlock()
	return copyNetworkMember(d.id2Member[pkiID.String()])
}

func copyNetworkMember(member *common.NetworkMember) *common.NetworkMember {
	if member == nil {
		return nil
	}

	copied := &common.NetworkMember{}
	*copied = *member
	return copied
}

func (d *gossipDiscoveryService) InitiateSync(peerNum int) {
	if d.toDie() {
		return
	}
	var peers2SendTo []*common.NetworkMember
	m, err := d.createMembershipRequest()
	if err != nil {
		logging.Warningf("Failed creating membership request: %+v", errors.WithStack(err))
		return
	}
	memReq, err := m.NoopSign()
	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", errors.WithStack(err))
		return
	}
	d.lock.RLock()

	n := d.aliveMembership.Size()
	k := peerNum
	if k > n {
		k = n
	}

	aliveMembersAsSlice := d.aliveMembership.ToSlice()
	for _, i := range util.GetRandomIndices(k, n-1) {
		pulledPeer := aliveMembersAsSlice[i].GetAliveMsg().Membership
		netMember := &common.NetworkMember{
			Endpoint: pulledPeer.Endpoint,
			PKIID:    pulledPeer.PkiId,
		}
		peers2SendTo = append(peers2SendTo, netMember)
	}

	d.lock.RUnlock()

	for _, member := range peers2SendTo {
		d.rpc.SendToPeer(member, memReq)
	}
}

func (d *gossipDiscoveryService) GetMembership() []common.NetworkMember {
	if d.toDie() {
		return []common.NetworkMember{}
	}
	d.lock.RLock()
	defer d.lock.RUnlock()

	response := []common.NetworkMember{}
	for _, m := range d.aliveMembership.ToSlice() {
		member := m.GetAliveMsg()
		response = append(response, common.NetworkMember{Endpoint: member.Membership.Endpoint, PKIID: member.Membership.PkiId})
	}
	return response
}

func (d *gossipDiscoveryService) Stop() {
	defer logging.Info("Stopped")
	logging.Info("Stopping")

	d.lock.Lock()
	defer d.lock.Unlock()

	atomic.StoreInt32(&d.toDieFlag, int32(1))
	d.msgStore.Stop()
	d.toDieChan <- struct{}{}
}

func (d *gossipDiscoveryService) sendUntilAcked(peer *common.NetworkMember, message *protos.SignedRKSyncMessage) {
	nonce := message.Nonce
	for i := 0; i < maxConnectionAttempts && !d.toDie(); i++ {
		sub := d.pubsub.Subscribe(fmt.Sprintf("%d", nonce), time.Second*5)
		d.rpc.SendToPeer(peer, message)
		if _, timeoutErr := sub.Listen(); timeoutErr == nil {
			return
		}
		time.Sleep(d.reconnectInterval)
	}
}

func (d *gossipDiscoveryService) createMembershipResponse(aliveMsg *protos.SignedRKSyncMessage, target *common.NetworkMember) *protos.MembershipResponse {
	shouldBeDisclosed, omitConcealedFields := d.disclosurePolicy(target)
	if !shouldBeDisclosed(aliveMsg) {
		return nil
	}

	d.lock.RLock()
	defer d.lock.RUnlock()

	deadPeers := []*protos.Envelope{}

	for _, dm := range d.deadMembership.ToSlice() {
		if !shouldBeDisclosed(dm) {
			continue
		}
		deadPeers = append(deadPeers, omitConcealedFields(dm))
	}

	aliveSnapshot := []*protos.Envelope{}
	for _, am := range d.aliveMembership.ToSlice() {
		if !shouldBeDisclosed(am) {
			continue
		}
		aliveSnapshot = append(aliveSnapshot, omitConcealedFields(am))
	}

	return &protos.MembershipResponse{
		Alive: append(aliveSnapshot, omitConcealedFields(aliveMsg)),
		Dead:  deadPeers,
	}
}

func (d *gossipDiscoveryService) createMembershipRequest() (*protos.RKSyncMessage, error) {
	am, err := d.createSignedAliveMessage()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	req := &protos.MembershipRequest{
		SelfInformation: am.Envelope,
		// TODO: sending the known peers
		Known: [][]byte{},
	}

	return &protos.RKSyncMessage{
		Tag:   protos.RKSyncMessage_EMPTY,
		Nonce: uint64(0),
		Content: &protos.RKSyncMessage_MemReq{
			MemReq: req,
		},
	}, nil
}

func (d *gossipDiscoveryService) createSignedAliveMessage() (*protos.SignedRKSyncMessage, error) {
	msg := d.aliveMsg()
	envp := d.crypt.SignMessage(msg)
	if envp == nil {
		return nil, errors.New("Failed signing message")
	}
	signedMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: msg,
		Envelope:      envp,
	}
	return signedMsg, nil
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

func (d *gossipDiscoveryService) isMyOwnEndpoint(endpoint string) bool {
	// return strings.Contains(endpoint, "127.0.0.1") || strings.Contains(endpoint, "localhost") ||
	// 	endpoint == d.self.Endpoint

	return endpoint == d.self.Endpoint
}

func (d *gossipDiscoveryService) toDie() bool {
	return atomic.LoadInt32(&d.toDieFlag) == int32(1)
}

func (d *gossipDiscoveryService) getDeadMembers() []common.PKIidType {
	d.lock.RLock()
	defer d.lock.RUnlock()

	dead := []common.PKIidType{}
	for id, last := range d.aliveLastTS {
		elapsedNonAliveTime := time.Since(last.lastSeen)
		if elapsedNonAliveTime > d.aliveExpirationTimeout {
			logging.Warning("Haven't heard from", id, "for", elapsedNonAliveTime)
			dead = append(dead, common.PKIidType(id))
		}
	}
	return dead
}

func (d *gossipDiscoveryService) periodicalSendAlive() {
	defer logging.Debug("Stopped")

	for !d.toDie() {
		select {
		case <-time.After(d.aliveTimeInterval):
			msg, err := d.createSignedAliveMessage()
			if err != nil {
				logging.Warningf("Failed creating alive message: %+v", errors.WithStack(err))
				return
			}
			d.lock.Lock()
			d.selfAliveMessage = msg
			d.lock.Unlock()
			d.rpc.Gossip(msg)
		case s := <-d.toDieChan:
			d.toDieChan <- s
			return
		}
	}
}

func (d *gossipDiscoveryService) periodicalCheckAlive() {
	defer logging.Debug("Stopped")

	for !d.toDie() {
		select {
		case <-time.After(d.aliveExpirationCheckInterval):
			dead := d.getDeadMembers()
			if len(dead) > 0 {
				logging.Debug("Got %d dead members: %v", len(dead), dead)
				d.expireDeadMembers(dead)
			}
		case s := <-d.toDieChan:
			d.toDieChan <- s
			return
		}
	}
}

func (d *gossipDiscoveryService) expireDeadMembers(dead []common.PKIidType) {
	logging.Warning("Entering", dead)
	defer logging.Warning("Exiting")

	var deadMembers2Expire []*common.NetworkMember

	d.lock.Lock()

	for _, pkiID := range dead {
		if _, isAlive := d.aliveLastTS[pkiID.String()]; !isAlive {
			continue
		}
		deadMembers2Expire = append(deadMembers2Expire, d.id2Member[pkiID.String()])
		lastTS, hasLastTS := d.aliveLastTS[pkiID.String()]
		if hasLastTS {
			d.deadLastTS[pkiID.String()] = lastTS
			delete(d.aliveLastTS, pkiID.String())
		}

		if am := d.aliveMembership.MsgByID(pkiID); am != nil {
			d.deadMembership.Put(pkiID, am)
			d.aliveMembership.Remove(pkiID)
		}
	}

	d.lock.Unlock()

	for _, member2Expire := range deadMembers2Expire {
		logging.Warning("Closing connection to", member2Expire)
		d.rpc.CloseConn(member2Expire)
	}
}

func (d *gossipDiscoveryService) handleMessage() {
	defer logging.Debug("Stopped")

	in := d.rpc.Accept()
	for !d.toDie() {
		select {
		case s := <-d.toDieChan:
			d.toDieChan <- s
			return
		case m := <-in:
			d.handleMsgFromRPC(m)
		}
	}
}

func (d *gossipDiscoveryService) handleMsgFromRPC(msg protos.ReceivedMessage) {
	if msg == nil {
		return
	}
	m := msg.GetRKSyncMessage()
	if m.GetAliveMsg() == nil && m.GetMemRes() == nil && m.GetMemReq() == nil {
		logging.Warning("Got message with wrong type (expected Alive or MembershipResponse or MembershipRequest message)", m.RKSyncMessage)
		return
	}

	logging.Debug("Got message:", m)
	defer logging.Debug("Exiting")

	if memReq := m.GetMemReq(); memReq != nil {
		selfInRKSyncMsg, err := memReq.SelfInformation.ToRKSyncMessage()
		if err != nil {
			logging.Warningf("Failed deserializing RKSyncMessage from envelope: %+v", errors.WithStack(err))
			return
		}
		if !d.crypt.ValidateAliveMsg(selfInRKSyncMsg) {
			logging.Warningf("Failed validating alive message: %+v", selfInRKSyncMsg)
			return
		}
		if d.msgStore.CheckValid(selfInRKSyncMsg) {
			d.handleAliveMessage(selfInRKSyncMsg)
		}

		go d.sendMemResponse(selfInRKSyncMsg.GetAliveMsg().Membership, m.Nonce)
		return
	}

	if m.IsAliveMsg() {
		if !d.msgStore.CheckValid(m) || !d.crypt.ValidateAliveMsg(m) {
			return
		}
		if d.isSentByMe(m) {
			return
		}

		d.msgStore.Add(m)
		d.handleAliveMessage(m)
		d.rpc.Forward(msg)
		return
	}

	if memResp := m.GetMemRes(); memResp != nil {
		d.pubsub.Publish(fmt.Sprintf("%d", m.Nonce), m.Nonce)
		for _, env := range memResp.Alive {
			am, err := env.ToRKSyncMessage()
			if err != nil {
				logging.Warningf("Membership response contains an invalid message from an online peer: %+v", errors.WithStack(err))
				return
			}
			if !am.IsAliveMsg() {
				logging.Warning("Expected alive message, got", am, "instead")
				return
			}
			if d.msgStore.CheckValid(am) && d.crypt.ValidateAliveMsg(am) {
				d.handleAliveMessage(am)
			}
		}

		for _, env := range memResp.Dead {
			dm, err := env.ToRKSyncMessage()
			if err != nil {
				logging.Warningf("Membership response contains an invalid message from an online peer %+v", errors.WithStack(err))
				return
			}
			if !d.msgStore.CheckValid(dm) || !d.crypt.ValidateAliveMsg(dm) {
				continue
			}

			newDeadMembers := []*protos.SignedRKSyncMessage{}
			d.lock.RLock()
			if _, known := d.id2Member[common.PKIidType(dm.GetAliveMsg().Membership.PkiId).String()]; !known {
				newDeadMembers = append(newDeadMembers, dm)
			}
			d.lock.RUnlock()
			d.learnNewMembers([]*protos.SignedRKSyncMessage{}, newDeadMembers)
		}
	}
}

func (d *gossipDiscoveryService) handleAliveMessage(m *protos.SignedRKSyncMessage) {
	logging.Debug("Entering", m)
	defer logging.Debug("Exiting")

	if d.isSentByMe(m) {
		return
	}

	pkiID := common.PKIidType(m.GetAliveMsg().Membership.PkiId)
	ts := m.GetAliveMsg().Timestamp

	d.lock.RLock()
	_, known := d.id2Member[pkiID.String()]
	d.lock.RUnlock()

	if !known {
		d.learnNewMembers([]*protos.SignedRKSyncMessage{m}, []*protos.SignedRKSyncMessage{})
		return
	}

	d.lock.RLock()
	_, isAlive := d.aliveLastTS[pkiID.String()]
	lastDeadTS, isDead := d.deadLastTS[pkiID.String()]
	d.lock.RUnlock()

	if !isAlive && !isDead {
		logging.Fatalf("Member %s is known but not found neither in alive nor in dead lastTS maps, isAlive=%v, isDead=%v", m.GetAliveMsg().Membership.Endpoint, isAlive, isDead)
		return
	}

	if isAlive && isDead {
		logging.Fatalf("Member %s is both alive and dead at the same time", m.GetAliveMsg().Membership.Endpoint)
		return
	}

	if isDead {
		if before(lastDeadTS, ts) {
			d.resurrectMember(m, *ts)
		} else if !same(lastDeadTS, ts) {
			logging.Debug(m.GetAliveMsg().Membership, "lastDeadTS:", lastDeadTS, "but got ts:", ts)
		}
		return
	}

	d.lock.RLock()
	lastAliveTS, isAlive := d.aliveLastTS[pkiID.String()]
	d.lock.RUnlock()

	if isAlive {
		if before(lastAliveTS, ts) {
			d.learnExistingMembers([]*protos.SignedRKSyncMessage{m})
		} else if !same(lastAliveTS, ts) {
			logging.Debug(m.GetAliveMsg().Membership, "lastAliveTS:", lastAliveTS, "but got ts:", ts)
		}
	}
}

func (d *gossipDiscoveryService) sendMemResponse(target *protos.Member, nonce uint64) {
	logging.Debug("Entering", target)

	targetPeer := &common.NetworkMember{
		Endpoint: target.Endpoint,
		PKIID:    target.PkiId,
	}

	var aliveMsg *protos.SignedRKSyncMessage
	var err error
	d.lock.RLock()
	aliveMsg = d.selfAliveMessage
	d.lock.RUnlock()

	if aliveMsg == nil {
		aliveMsg, err = d.createSignedAliveMessage()
		if err != nil {
			logging.Warningf("Failed creating alive message: %+v", errors.WithStack(err))
			return
		}
	}

	memResp := d.createMembershipResponse(aliveMsg, targetPeer)
	if memResp == nil {
		logging.Warningf("Got a membership request from a peer that shouldn't have sent one: %v, closing connection to the peer as a result", target)
		d.rpc.CloseConn(targetPeer)
		return
	}

	defer logging.Debug("Exiting, replying with", memResp)

	msg, err := (&protos.RKSyncMessage{
		Tag:   protos.RKSyncMessage_EMPTY,
		Nonce: nonce,
		Content: &protos.RKSyncMessage_MemRes{
			MemRes: memResp,
		},
	}).NoopSign()

	if err != nil {
		logging.Warningf("Failed creating SignedRKSyncMessage: %+v", errors.WithStack(err))
		return
	}
	d.rpc.SendToPeer(targetPeer, msg)
}

func (d *gossipDiscoveryService) learnExistingMembers(aliveArr []*protos.SignedRKSyncMessage) {
	logging.Debugf("Entering: learnedMembers={%v}", aliveArr)
	defer logging.Debug("Exiting")

	d.lock.Lock()
	defer d.lock.Unlock()

	for _, m := range aliveArr {
		am := m.GetAliveMsg()
		if am == nil {
			logging.Warning("Expecting alive message, got instead:", m)
			return
		}
		logging.Debug("updating", am)

		member := d.id2Member[common.PKIidType(am.Membership.PkiId).String()]
		member.Endpoint = am.Membership.Endpoint

		if _, isKnownAsDead := d.deadLastTS[common.PKIidType(am.Membership.PkiId).String()]; isKnownAsDead {
			logging.Warning(am.Membership, "has already expired")
			continue
		}

		if _, isKnownAsAlive := d.aliveLastTS[common.PKIidType(am.Membership.PkiId).String()]; !isKnownAsAlive {
			logging.Warning(am.Membership, "has already expired")
			continue
		} else {
			logging.Debug("Updating aliveness data:", am)
			alive := d.aliveLastTS[common.PKIidType(am.Membership.PkiId).String()]
			alive.incTime = tsToTime(am.Timestamp.IncNum)
			alive.lastSeen = time.Now()
			alive.seqNum = am.Timestamp.SeqNum

			if am := d.aliveMembership.MsgByID(m.GetAliveMsg().Membership.PkiId); am == nil {
				logging.Debug("Adding", am, "to aliveMembership")
				msg := &protos.SignedRKSyncMessage{RKSyncMessage: m.RKSyncMessage, Envelope: m.Envelope}
				d.aliveMembership.Put(m.GetAliveMsg().Membership.PkiId, msg)
			} else {
				logging.Debug("Replacing", am, "in aliveMembership")
				am.RKSyncMessage = m.RKSyncMessage
				am.Envelope = m.Envelope
			}
		}
	}
}

func (d *gossipDiscoveryService) resurrectMember(am *protos.SignedRKSyncMessage, t protos.PeerTime) {
	logging.Debug("Entering, AliveMessage:", am, "t:", t)
	defer logging.Debug("Exiting")
	d.lock.Lock()
	defer d.lock.Unlock()

	member := am.GetAliveMsg().Membership
	pkiID := member.PkiId
	d.aliveLastTS[common.PKIidType(pkiID).String()] = &timestamp{
		lastSeen: time.Now(),
		seqNum:   t.SeqNum,
		incTime:  tsToTime(t.IncNum),
	}

	d.id2Member[common.PKIidType(pkiID).String()] = &common.NetworkMember{
		Endpoint: member.Endpoint,
		PKIID:    member.PkiId,
	}

	delete(d.deadLastTS, common.PKIidType(pkiID).String())
	d.deadMembership.Remove(common.PKIidType(pkiID))
	d.aliveMembership.Put(common.PKIidType(pkiID), &protos.SignedRKSyncMessage{RKSyncMessage: am.RKSyncMessage, Envelope: am.Envelope})
}

func (d *gossipDiscoveryService) learnNewMembers(aliveMembers []*protos.SignedRKSyncMessage, deadMembers []*protos.SignedRKSyncMessage) {
	logging.Debugf("Entering: learnedMembers={%v}, deadMembers={%v}", aliveMembers, deadMembers)
	defer logging.Debug("Exiting")

	d.lock.Lock()
	defer d.lock.Unlock()

	for _, am := range aliveMembers {
		if equalPKIid(am.GetAliveMsg().Membership.PkiId, d.self.PKIID) {
			continue
		}
		d.aliveLastTS[common.PKIidType(am.GetAliveMsg().Membership.PkiId).String()] = &timestamp{
			incTime:  tsToTime(am.GetAliveMsg().Timestamp.IncNum),
			lastSeen: time.Now(),
			seqNum:   am.GetAliveMsg().Timestamp.SeqNum,
		}

		d.aliveMembership.Put(am.GetAliveMsg().Membership.PkiId, &protos.SignedRKSyncMessage{RKSyncMessage: am.RKSyncMessage, Envelope: am.Envelope})
		logging.Debugf("Learned about a new alive member: %v", am)
	}

	for _, dm := range deadMembers {
		if equalPKIid(dm.GetAliveMsg().Membership.PkiId, d.self.PKIID) {
			continue
		}
		d.deadLastTS[common.PKIidType(dm.GetAliveMsg().Membership.PkiId).String()] = &timestamp{
			incTime:  tsToTime(dm.GetAliveMsg().Timestamp.IncNum),
			lastSeen: time.Now(),
			seqNum:   dm.GetAliveMsg().Timestamp.SeqNum,
		}

		d.deadMembership.Put(dm.GetAliveMsg().Membership.PkiId, &protos.SignedRKSyncMessage{RKSyncMessage: dm.RKSyncMessage, Envelope: dm.Envelope})
		logging.Debugf("Learned about a new dead member: %v", dm)
	}

	for _, a := range [][]*protos.SignedRKSyncMessage{aliveMembers, deadMembers} {
		for _, m := range a {
			member := m.GetAliveMsg()
			if member == nil {
				logging.Warning("Expected alive message, got instead:", m)
				return
			}

			d.id2Member[common.PKIidType(member.Membership.PkiId).String()] = &common.NetworkMember{
				Endpoint: member.Membership.Endpoint,
				PKIID:    member.Membership.PkiId,
			}
		}
	}
}

func (d *gossipDiscoveryService) isSentByMe(m *protos.SignedRKSyncMessage) bool {
	pkiID := m.GetAliveMsg().Membership.PkiId
	if !equalPKIid(pkiID, d.self.PKIID) {
		return false
	}

	logging.Debug("Got alive message about ourselves,", m)
	diffEndpoint := d.self.Endpoint != m.GetAliveMsg().Membership.Endpoint
	if diffEndpoint {
		logging.Error("Bad configuration detected: Received AliveMessage from a peer with the same PKI-ID as myself: ", m.RKSyncMessage)
	}
	return true
}

func (d *gossipDiscoveryService) periodicalReconnectToDead() {
	defer logging.Debug("Stopped")

	for !d.toDie() {
		select {
		case <-time.After(d.reconnectInterval):
			wg := sync.WaitGroup{}
			for _, member := range d.copyLastSeen(d.deadLastTS) {
				wg.Add(1)
				go func(member common.NetworkMember) {
					defer wg.Done()
					if d.rpc.Ping(&member) {
						logging.Debug(member, "is responding, sending membership request")
						d.sendMembershipRequest(&member)
					} else {
						logging.Debug(member, "is still dead")
					}
				}(member)
			}
			wg.Wait()
		case s := <-d.toDieChan:
			d.toDieChan <- s
			return
		}
	}
}

func (d *gossipDiscoveryService) sendMembershipRequest(member *common.NetworkMember) {
	m, err := d.createMembershipRequest()
	if err != nil {
		logging.Warning("Failed creating membership request: %+v", errors.WithStack(err))
		return
	}
	req, err := m.NoopSign()
	if err != nil {
		logging.Errorf("Failed creating SignedRKSyncMessage: %+v", errors.WithStack(err))
		return
	}
	d.rpc.SendToPeer(member, req)
}

func (d *gossipDiscoveryService) copyLastSeen(lastSeenMap map[string]*timestamp) []common.NetworkMember {
	d.lock.RLock()
	defer d.lock.RUnlock()

	res := []common.NetworkMember{}
	for pkiIDStr := range lastSeenMap {
		res = append(res, *(d.id2Member[pkiIDStr]))
	}
	return res
}

func (d *gossipDiscoveryService) handlePresumedDeadPeers() {
	defer logging.Debug("Stopped")

	for !d.toDie() {
		select {
		case deadPeer := <-d.rpc.PresumedDead():
			if d.isAlive(deadPeer) {
				d.expireDeadMembers([]common.PKIidType{deadPeer})
			}
		case s := <-d.toDieChan:
			d.toDieChan <- s
			return
		}
	}
}

func (d *gossipDiscoveryService) isAlive(pkiID common.PKIidType) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()
	_, alive := d.aliveLastTS[pkiID.String()]
	return alive
}

type aliveMsgStore struct {
	lib.MessageStore
}

func newAliveMsgStore(d *gossipDiscoveryService) *aliveMsgStore {
	policy := protos.NewRKSyncMessageComparator()
	trigger := func(m interface{}) {}
	aliveMsgTTL := d.aliveExpirationTimeout * msgExpirationFactor
	externalLock := func() { d.lock.Lock() }
	externalUnlock := func() { d.lock.Unlock() }
	callback := func(m interface{}) {
		msg := m.(*protos.SignedRKSyncMessage)
		if !msg.IsAliveMsg() {
			return
		}
		id := msg.GetAliveMsg().Membership.PkiId
		d.aliveMembership.Remove(id)
		d.deadMembership.Remove(id)
		delete(d.id2Member, common.PKIidType(id).String())
		delete(d.deadLastTS, common.PKIidType(id).String())
		delete(d.aliveLastTS, common.PKIidType(id).String())
	}

	s := &aliveMsgStore{
		MessageStore: lib.NewMessageStoreExpirable(policy, trigger, aliveMsgTTL, externalLock, externalUnlock, callback),
	}

	return s
}

func (s *aliveMsgStore) Add(msg interface{}) bool {
	if !msg.(*protos.SignedRKSyncMessage).IsAliveMsg() {
		panic(fmt.Sprint("Message ", msg, "is not AliveMsg"))
	}
	return s.MessageStore.Add(msg)
}

func (s *aliveMsgStore) CheckValid(msg interface{}) bool {
	if !msg.(*protos.SignedRKSyncMessage).IsAliveMsg() {
		panic(fmt.Sprint("Message ", msg, "is not AliveMsg"))
	}
	return s.MessageStore.CheckValid(msg)
}

func equalPKIid(a, b common.PKIidType) bool {
	return bytes.Equal(a, b)
}

func tsToTime(ts uint64) time.Time {
	return time.Unix(int64(0), int64(ts))
}

func same(a *timestamp, b *protos.PeerTime) bool {
	return uint64(a.incTime.UnixNano()) == b.IncNum && a.seqNum == b.SeqNum
}

func before(a *timestamp, b *protos.PeerTime) bool {
	return (uint64(a.incTime.UnixNano()) == b.IncNum && a.seqNum < b.SeqNum) ||
		uint64(a.incTime.UnixNano()) < b.IncNum
}
