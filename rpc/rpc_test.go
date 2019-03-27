/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"math/rand"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
)

func TestSendWithAck(t *testing.T) {
	acceptor := func(o interface{}) bool {
		return o.(protos.ReceivedMessage).GetRKSyncMessage().IsAliveMsg()
	}

	inc1 := inst1.Accept(acceptor)
	inc2 := inst2.Accept(acceptor)

	inst1.Send(createRKSyncMessage(), &common.NetworkMember{Endpoint: "localhost:10053", PKIID: []byte("localhost:10053")})
	<-inc2

	msgNum := 1000
	for i := 0; i < msgNum; i++ {
		go inst1.SendWithAck(createRKSyncMessage(), time.Second*5, 1, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: []byte("localhost:10053")})
	}

	for i := 0; i < msgNum; i++ {
		go inst2.SendWithAck(createRKSyncMessage(), time.Second*5, 1, &common.NetworkMember{Endpoint: "localhost:9053", PKIID: []byte("localhost:9053")})
	}

	go func() {
		for i := 0; i < msgNum; i++ {
			<-inc1
		}
	}()

	go func() {
		for i := 0; i < msgNum; i++ {
			<-inc2
		}
	}()
}

func TestGetConnectionInfo(t *testing.T) {
	m1 := inst1.Accept(func(o interface{}) bool { return true })
	inst2.Send(createRKSyncMessage(), &common.NetworkMember{Endpoint: "localhost:9053", PKIID: []byte("localhost:9053")})
	select {
	case <-time.After(time.Second * 5):
		t.Fatal("Didn't receive a message in time")
	case msg := <-m1:
		assert.Equal(t, inst2.GetPKIid(), msg.GetConnectionInfo().ID)
		assert.NotNil(t, msg.GetSourceEnvelope())
	}
}

func TestHandshake(t *testing.T) {
	_, err := inst1.Handshake(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: []byte("localhost:10053")})
	assert.Error(t, err, "PKI-ID of remote peer doesn't match expected PKI-ID")

	id, err := inst1.Handshake(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	assert.NoError(t, err)
	sid := &protos.SerializedIdentity{}
	err = proto.Unmarshal(id, sid)
	assert.NoError(t, err)

	assert.Equal(t, "peer1.org2", sid.NodeId)
}

func TestNonResponsivePing(t *testing.T) {
	s := make(chan error)
	go func() {
		err := inst1.Probe(&common.NetworkMember{Endpoint: "localhost:10053", PKIID: []byte("localhost:10053")})
		s <- err
	}()

	select {
	case <-time.After(time.Second * 10):
		assert.Fail(t, "Request wasn't cancelled on time")
	case err := <-s:
		assert.Nil(t, err)
	}
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
