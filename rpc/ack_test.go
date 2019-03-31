package rpc

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/stretchr/testify/assert"
)

func TestInterceptAcks(t *testing.T) {
	pubsub := lib.NewPubSub()
	pkiID := common.PKIidType("peer0.org1")
	msgs := make(chan *protos.SignedRKSyncMessage, 1)
	handlerFunc := func(message *protos.SignedRKSyncMessage) {
		msgs <- message
	}

	wrappedHandler := interceptAcks(handlerFunc, pkiID, pubsub)
	ack := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Nonce: 1,
			Content: &protos.RKSyncMessage_Ack{
				Ack: &protos.Acknowledgement{},
			},
		},
	}

	sub := pubsub.Subscribe(topicForAck(1, pkiID), time.Second)
	wrappedHandler(ack)

	assert.Len(t, msgs, 0)
	_, err := sub.Listen()
	assert.NoError(t, err)

	notAck := &protos.SignedRKSyncMessage{
		RKSyncMessage: &protos.RKSyncMessage{
			Nonce:   2,
			Content: &protos.RKSyncMessage_DataMsg{},
		},
	}

	sub = pubsub.Subscribe(topicForAck(2, pkiID), time.Second)
	wrappedHandler(notAck)

	assert.Len(t, msgs, 1)
	_, err = sub.Listen()
	assert.Error(t, err)
}

func TestAck(t *testing.T) {
	acceptData := func(o interface{}) bool {
		return o.(protos.ReceivedMessage).GetRKSyncMessage().IsAliveMsg()
	}

	ack := func(c <-chan protos.ReceivedMessage) {
		msg := <-c
		msg.Ack(nil)
	}

	nack := func(c <-chan protos.ReceivedMessage) {
		msg := <-c
		msg.Ack(errors.New("Failed processing message because reasons"))
	}

	inc2 := inst2.Accept(acceptData)
	go ack(inc2)

	res := inst1.SendWithAck(createRKSyncMessage(), time.Second*3, 2, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	assert.Len(t, res, 1)
	assert.Empty(t, res[0].Error())

	t1 := time.Now()
	go ack(inc2)
	res = inst1.SendWithAck(createRKSyncMessage(), time.Second*10, 2, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	elapsed := time.Since(t1)
	assert.Len(t, res, 1)
	assert.Empty(t, res[0].Error())
	assert.True(t, elapsed < time.Second*5)

	go nack(inc2)
	res = inst1.SendWithAck(createRKSyncMessage(), time.Second*10, 2, &common.NetworkMember{Endpoint: "localhost:10053", PKIID: inst2.GetPKIid()})
	assert.Len(t, res, 1)
	assert.Contains(t, res[0].Error(), "Failed processing message because reasons")
}
