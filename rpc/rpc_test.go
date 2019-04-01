/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestSendWithAck(t *testing.T) {
	acceptor := func(o interface{}) bool {
		return o.(protos.ReceivedMessage).GetRKSyncMessage().IsAliveMsg()
	}

	inc1 := inst1.Accept(acceptor)
	inc2 := inst2.Accept(acceptor)

	inst1.Send(createRKSyncMessage(), &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	<-inc2

	msgNum := 1000
	for i := 0; i < msgNum; i++ {
		go inst1.SendWithAck(createRKSyncMessage(), time.Second*5, 1, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	}

	for i := 0; i < msgNum; i++ {
		go inst2.SendWithAck(createRKSyncMessage(), time.Second*5, 1, &common.NetworkMember{Endpoint: "localhost:9053", PKIID: inst1.GetPKIid()})
	}

	go func() {
		for i := 0; i < msgNum; i++ {
			<-inc1
		}
	}()

	for i := 0; i < msgNum; i++ {
		<-inc2
	}
}

func TestGetConnectionInfo(t *testing.T) {
	m1 := inst1.Accept(func(o interface{}) bool { return true })
	inst2.Send(createRKSyncMessage(), &common.NetworkMember{Endpoint: "localhost:9053", PKIID: inst1.GetPKIid()})
	select {
	case <-time.After(time.Second * 5):
		t.Fatal("Didn't receive a message in time")
	case msg := <-m1:
		assert.Equal(t, inst2.GetPKIid(), msg.GetConnectionInfo().ID)
		assert.NotNil(t, msg.GetSourceEnvelope())
	}
}

func TestHandshake(t *testing.T) {

	home1, err := filepath.Abs("../tests/fixtures/identity/peer0")
	require.NoError(t, err)

	cfg1 := &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = cfg1.MakeFilesAbs(home1)
	require.NoError(t, err)

	inst1, srv1, err := CreateRPCServer("localhost:9054", cfg1)
	require.NoError(t, err)

	home2, err := filepath.Abs("../tests/fixtures/identity/peer1")
	require.NoError(t, err)

	cfg2 := &config.IdentityConfig{
		ID: "peer1.org2",
	}
	err = cfg2.MakeFilesAbs(home2)
	require.NoError(t, err)

	inst2, srv2, err := CreateRPCServer("localhost:10054", cfg2)
	require.NoError(t, err)

	go srv1.Start()
	defer srv1.Stop()
	go srv2.Start()
	defer srv2.Stop()

	_, err = inst1.Handshake(&common.NetworkMember{Endpoint: "localhost:10054", PKIID: inst1.GetPKIid()})
	assert.Error(t, err, "PKI-ID of remote peer doesn't match expected PKI-ID")

	id, err := inst1.Handshake(&common.NetworkMember{Endpoint: "localhost:10054", PKIID: inst2.GetPKIid()})
	assert.NoError(t, err)
	sid := &protos.SerializedIdentity{}
	err = proto.Unmarshal(id, sid)
	assert.NoError(t, err)
	assert.Equal(t, "peer1.org2", sid.NodeId)
}

func TestNonResponsivePing(t *testing.T) {
	s := make(chan error)
	go func() {
		err := inst1.Probe(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
		s <- err
	}()

	select {
	case <-time.After(time.Second * 10):
		assert.Fail(t, "Request wasn't cancelled on time")
	case err := <-s:
		assert.Nil(t, err)
	}
}

func TestPresumedDead(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Wait()
		msg := createRKSyncMessage()
		inst1.Send(msg, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	}()

	ticker := time.NewTicker(time.Duration(10) * time.Second)
	acceptCh := inst2.Accept(func(o interface{}) bool { return true })
	wg.Done()
	select {
	case <-acceptCh:
		ticker.Stop()
	case <-ticker.C:
		assert.Fail(t, "Didn't get first message")
	}

	go func() {
		for i := 0; i < 5; i++ {
			inst1.Send(createRKSyncMessage(), &common.NetworkMember{Endpoint: "localhost:8053", PKIID: []byte("peer2.org3")})
			time.Sleep(time.Millisecond * 200)
		}
	}()

	ticker = time.NewTicker(time.Second * time.Duration(5))
	select {
	case <-ticker.C:
		assert.Fail(t, "Didn't get a presumed dead message within a timely manner")
		break
	case <-inst1.PresumedDead():
		ticker.Stop()
		break
	}
}

func TestProbe(t *testing.T) {
	time.Sleep(1 * time.Second)
	assert.NoError(t, inst1.Probe(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()}))

	_, err := inst1.Handshake(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	assert.NoError(t, err)
	assert.Error(t, inst1.Probe(&common.NetworkMember{Endpoint: "localhost:11053", PKIID: []byte("localhost:11053")}))
}

func createRKSyncMessage() *protos.SignedRKSyncMessage {
	msg, _ := (&protos.RKSyncMessage{
		Tag:   protos.RKSyncMessage_EMPTY,
		Nonce: uint64(rand.Int()),
		Content: &protos.RKSyncMessage_AliveMsg{
			AliveMsg: &protos.AliveMessage{},
		},
	}).NoopSign()

	return msg
}
