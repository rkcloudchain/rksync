/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"google.golang.org/grpc"
)

type handler func(message *protos.SignedRKSyncMessage)
type connCreation func(endpoint string, pkiID common.PKIidType) (*connection, error)

type connectionStore struct {
	isClosing    bool
	connCreation connCreation
	sync.RWMutex
	conns            map[string]*connection
	destinationLocks map[string]*sync.Mutex
}

func newConnStore(connCreation connCreation) *connectionStore {
	return &connectionStore{
		connCreation:     connCreation,
		isClosing:        false,
		conns:            make(map[string]*connection),
		destinationLocks: make(map[string]*sync.Mutex),
	}
}

func (cs *connectionStore) getConnection(peer *common.NetworkMember) (*connection, error) {
	cs.RLock()
	isClosing := cs.isClosing
	cs.RUnlock()

	if isClosing {
		return nil, errors.Errorf("Shutting down")
	}

	pkiID := peer.PKIID
	endpoint := peer.Endpoint

	cs.Lock()
	destinationLock, hasConnected := cs.destinationLocks[pkiID.String()]
	if !hasConnected {
		destinationLock = &sync.Mutex{}
		cs.destinationLocks[pkiID.String()] = destinationLock
	}
	cs.Unlock()

	destinationLock.Lock()

	cs.RLock()
	conn, exists := cs.conns[pkiID.String()]
	if exists {
		cs.RUnlock()
		destinationLock.Unlock()
		return conn, nil
	}
	cs.RUnlock()

	createdConnection, err := cs.connCreation(endpoint, pkiID)
	destinationLock.Unlock()

	cs.RLock()
	isClosing = cs.isClosing
	cs.RUnlock()
	if isClosing {
		return nil, errors.Errorf("ConnStore is closing")
	}

	cs.Lock()
	delete(cs.destinationLocks, pkiID.String())
	defer cs.Unlock()

	conn, exists = cs.conns[pkiID.String()]
	if exists {
		if createdConnection != nil {
			createdConnection.close()
		}
		return conn, nil
	}

	if err != nil {
		return nil, err
	}

	conn = createdConnection
	cs.conns[createdConnection.info.ID.String()] = conn
	go conn.serviceConnection()

	return conn, nil
}

func (cs *connectionStore) connNum() int {
	cs.RLock()
	defer cs.RUnlock()
	return len(cs.conns)
}

func (cs *connectionStore) closeConn(peer *common.NetworkMember) {
	cs.Lock()
	defer cs.Unlock()

	if conn, exists := cs.conns[peer.PKIID.String()]; exists {
		conn.close()
		delete(cs.conns, conn.info.ID.String())
	}
}

func (cs *connectionStore) shutdown() {
	logging.Info("Closing rpc connection store")
	defer logging.Info("Closed rpc connection store")

	cs.Lock()
	cs.isClosing = true

	var connections2Close []*connection
	for _, conn := range cs.conns {
		connections2Close = append(connections2Close, conn)
	}
	cs.Unlock()

	wg := sync.WaitGroup{}
	for _, conn := range connections2Close {

		wg.Add(1)
		go func(conn *connection) {
			cs.closeByPKIid(conn.info.ID)
			wg.Done()
		}(conn)
	}
	wg.Wait()
}

func (cs *connectionStore) onConnected(serverStream protos.RKSync_SyncStreamServer, connInfo *protos.ConnectionInfo) *connection {
	cs.Lock()
	defer cs.Unlock()

	if c, exists := cs.conns[connInfo.ID.String()]; exists {
		c.close()
	}

	return cs.registerConn(connInfo, serverStream)
}

func (cs *connectionStore) registerConn(connInfo *protos.ConnectionInfo, serverStream protos.RKSync_SyncStreamServer) *connection {
	conn := newConnection(nil, nil, serverStream)
	conn.info = connInfo
	cs.conns[connInfo.ID.String()] = conn
	return conn
}

func (cs *connectionStore) closeByPKIid(pkiID common.PKIidType) {
	cs.Lock()
	defer cs.Unlock()
	if conn, exists := cs.conns[pkiID.String()]; exists {
		conn.close()
		delete(cs.conns, pkiID.String())
	}
}

func newConnection(c *grpc.ClientConn, cs protos.RKSync_SyncStreamClient, ss protos.RKSync_SyncStreamServer) *connection {
	connection := &connection{
		outBuff:      make(chan *msgSending, defSendBuffSize),
		conn:         c,
		clientStream: cs,
		serverStream: ss,
		stopFlat:     int32(0),
		stopChan:     make(chan struct{}, 1),
	}
	return connection
}

type connection struct {
	cancel       context.CancelFunc
	outBuff      chan *msgSending
	info         *protos.ConnectionInfo
	handler      handler
	conn         *grpc.ClientConn
	clientStream protos.RKSync_SyncStreamClient
	serverStream protos.RKSync_SyncStreamServer
	stopFlat     int32
	stopChan     chan struct{}
	stopWG       sync.WaitGroup
	sync.RWMutex
}

func (conn *connection) close() {
	if conn.toDie() {
		return
	}

	amIFirst := atomic.CompareAndSwapInt32(&conn.stopFlat, int32(0), int32(1))
	if !amIFirst {
		return
	}

	conn.stopChan <- struct{}{}
	conn.drainOutputBuffer()
	conn.Lock()
	defer conn.Unlock()

	if conn.clientStream != nil {
		conn.stopWG.Wait()
		conn.clientStream.CloseSend()
	}
	if conn.conn != nil {
		conn.conn.Close()
	}

	if conn.cancel != nil {
		conn.cancel()
	}
}

func (conn *connection) toDie() bool {
	return atomic.LoadInt32(&(conn.stopFlat)) == int32(1)
}

func (conn *connection) send(msg *protos.SignedRKSyncMessage, onErr func(error), shouldBlock bool) {
	if conn.toDie() {
		logging.Debug("Aborting send() to ", conn.info.Endpoint, " because connection is closing")
		return
	}

	m := &msgSending{
		envelope: msg.Envelope,
		onErr:    onErr,
	}

	if len(conn.outBuff) == cap(conn.outBuff) {
		logging.Debug("Buffer to ", conn.info.Endpoint, " overflowed, dropping message", msg.String())
		if !shouldBlock {
			return
		}
	}

	conn.outBuff <- m
}

func (conn *connection) serviceConnection() error {
	errChan := make(chan error, 1)
	msgChan := make(chan *protos.SignedRKSyncMessage, defRecvBuffSize)

	go conn.readFromStream(errChan, msgChan)

	conn.stopWG.Add(1)
	go conn.writeToStream()

	for !conn.toDie() {
		select {
		case stop := <-conn.stopChan:
			logging.Debug("Closing reading from stream")
			conn.stopChan <- stop
			return nil
		case err := <-errChan:
			return err
		case msg := <-msgChan:
			conn.handler(msg)
		}
	}
	return nil
}

func (conn *connection) writeToStream() {
	defer conn.stopWG.Done()
	for !conn.toDie() {
		stream := conn.getStream()
		if stream == nil {
			logging.Error(conn.info.ID, "Stream is nil, aborting!")
			return
		}

		select {
		case m := <-conn.outBuff:
			err := stream.Send(m.envelope)

			if err != nil {
				go m.onErr(err)
				return
			}
		case s := <-conn.stopChan:
			logging.Debug("Closing writing to stream")
			conn.stopChan <- s
			return
		}
	}
}

func (conn *connection) drainOutputBuffer() {
	for len(conn.outBuff) > 0 {
		<-conn.outBuff
	}
}

func (conn *connection) readFromStream(errChan chan error, msgChan chan *protos.SignedRKSyncMessage) {
	for !conn.toDie() {
		stream := conn.getStream()
		if stream == nil {
			logging.Error(conn.info.ID, "Stream is nil, aborting!")
			errChan <- errors.Errorf("Stream is nil")
			return
		}

		envelope, err := stream.Recv()
		if conn.toDie() {
			logging.Debug(conn.info.ID, "canceling read because closing")
			return
		}
		if err != nil {
			errChan <- err
			logging.Debugf("Got error, aborting: %v", err)
			return
		}

		msg, err := envelope.ToRKSyncMessage()
		if err != nil {
			errChan <- err
			logging.Debugf("Go error, aborting: %v", err)
			return
		}

		msgChan <- msg
	}
}

func (conn *connection) getStream() stream {
	conn.RLock()
	defer conn.RUnlock()

	if conn.toDie() {
		return nil
	}

	if conn.clientStream != nil && conn.serverStream != nil {
		logging.Error("Both client and server stream are not nil, something went wrong")
	}

	if conn.clientStream != nil {
		return conn.clientStream
	}

	if conn.serverStream != nil {
		return conn.serverStream
	}

	return nil
}

type stream interface {
	Send(envelope *protos.Envelope) error
	Recv() (*protos.Envelope, error)
	grpc.Stream
}

type msgSending struct {
	envelope *protos.Envelope
	onErr    func(error)
}
