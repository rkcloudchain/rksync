/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"encoding/json"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/protos"
)

type sendFunc func(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage)
type waitFunc func(*common.NetworkMember) error

type ackSendOperation struct {
	snd  sendFunc
	wait waitFunc
}

func newAckSendOperation(snd sendFunc, wait waitFunc) *ackSendOperation {
	return &ackSendOperation{snd: snd, wait: wait}
}

func (aso *ackSendOperation) send(msg *protos.SignedRKSyncMessage, minAckNum int, peers ...*common.NetworkMember) []SendResult {
	successAcks := 0
	results := []SendResult{}

	acks := make(chan SendResult, len(peers))
	for _, p := range peers {
		go func(p *common.NetworkMember) {
			aso.snd(p, msg)
			err := aso.wait(p)
			acks <- SendResult{NetworkMember: *p, error: err}
		}(p)
	}

	for {
		ack := <-acks
		results = append(results, SendResult{error: ack.error, NetworkMember: ack.NetworkMember})
		if ack.error == nil {
			successAcks++
		}
		if successAcks == minAckNum || len(results) == len(peers) {
			break
		}
	}
	return results
}

// SendResult defines a result of a send to a remote peer
type SendResult struct {
	error
	common.NetworkMember
}

// Error returns the error of the SendResult, or an empty string
// if an error hasn't occurred
func (sr SendResult) Error() string {
	if sr.error != nil {
		return sr.error.Error()
	}
	return ""
}

// AggregatedSendResult represents a slice of SendResults
type AggregatedSendResult []SendResult

// AckCount returns the number of successful acknowledgements
func (ar AggregatedSendResult) AckCount() int {
	c := 0
	for _, ack := range ar {
		if ack.error == nil {
			c++
		}
	}
	return c
}

// NackCount returns the number of unsuccessful acknowledgements
func (ar AggregatedSendResult) NackCount() int {
	return len(ar) - ar.AckCount()
}

// String returns a JSONed string representation
// of the AggregatedSendResult
func (ar AggregatedSendResult) String() string {
	errMap := map[string]int{}
	for _, ack := range ar {
		if ack.error == nil {
			continue
		}
		errMap[ack.Error()]++
	}

	ackCount := ar.AckCount()
	output := map[string]interface{}{}
	if ackCount > 0 {
		output["successes"] = ackCount
	}
	if ackCount < len(ar) {
		output["failures"] = errMap
	}
	b, _ := json.Marshal(output)
	return string(b)
}

func interceptAcks(nextHandler handler, remotePeerID common.PKIidType, pubSub *lib.PubSub) func(*protos.SignedRKSyncMessage) {
	return func(m *protos.SignedRKSyncMessage) {
		if m.IsAck() {
			topic := topicForAck(m.Nonce, remotePeerID)
			pubSub.Publish(topic, m.GetAck())
			return
		}
		nextHandler(m)
	}
}
