package rpc

import (
	"math/rand"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/protos"
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
		for i := 0; i< msgNum; i++ {
			<-inc2
		}
	}()
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
