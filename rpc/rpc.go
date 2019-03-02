package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/lib"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	handshakeTimeout = time.Second * time.Duration(10)
	defDialTimeout   = time.Second * time.Duration(3)
	defConnTimeout   = time.Second * time.Duration(2)
	defRecvBuffSize  = 20
	defSendBuffSize  = 20
)

// NewServer creates a new Server instance that binds itself to the given gRPC server
func NewServer(s *grpc.Server, idMapper identity.Identity, selfIdentity common.PeerIdentityType,
	secureDialOpts func() []grpc.DialOption) *Server {

	srv := &Server{
		pubSub:         lib.NewPubSub(),
		pkiID:          idMapper.GetPKIidOfCert(selfIdentity),
		idMapper:       idMapper,
		peerIdentity:   selfIdentity,
		secureDialOpts: secureDialOpts,
		gSrv:           s,
		msgPublisher:   NewChannelDemultiplexer(),
		deadEndpoints:  make(chan common.PKIidType, 100),
		stopping:       int32(0),
		exitChan:       make(chan struct{}),
		subscriptions:  make([]chan protos.ReceivedMessage, 0),
	}
	srv.connStore = newConnStore(srv.createConnection)
	protos.RegisterRKSyncServer(s, srv)
	return srv
}

// Server is an object that enables to communicate with other peers
type Server struct {
	secureDialOpts func() []grpc.DialOption
	pubSub         *lib.PubSub
	gSrv           *grpc.Server
	lsnr           net.Listener
	connStore      *connectionStore
	idMapper       identity.Identity
	peerIdentity   common.PeerIdentityType
	pkiID          common.PKIidType
	lock           sync.Mutex
	stopping       int32
	stopWG         sync.WaitGroup
	exitChan       chan struct{}
	deadEndpoints  chan common.PKIidType
	msgPublisher   *ChannelDeMultiplexer
	subscriptions  []chan protos.ReceivedMessage
}

func (s *Server) createConnection(endpoint string, expectedPKIID common.PKIidType) (*connection, error) {
	var err error
	var cc *grpc.ClientConn
	var stream protos.RKSync_SyncStreamClient
	var connInfo *protos.ConnectionInfo
	var dialOpts []grpc.DialOption

	logging.Debug("Entering", endpoint, expectedPKIID)
	defer logging.Debug("Exiting")

	if s.isStopping() {
		return nil, errors.New("Stopping")
	}

	dialOpts = append(dialOpts, s.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, defDialTimeout)
	defer cancel()

	cc, err = grpc.DialContext(ctx, endpoint, dialOpts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cl := protos.NewRKSyncClient(cc)
	ctx, cancel = context.WithTimeout(context.Background(), defConnTimeout)
	defer cancel()
	if _, err = cl.Ping(ctx, &empty.Empty{}); err != nil {
		cc.Close()
		return nil, errors.WithStack(err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	if stream, err = cl.SyncStream(ctx); err == nil {
		connInfo, err = s.authenticateRemotePeer(stream)
		if err == nil {
			if expectedPKIID != nil && !bytes.Equal(connInfo.ID, expectedPKIID) {
				logging.Warning("Remote endpoint claims to be a different peer, expected", expectedPKIID, "but got", connInfo.ID)
			}

			conn := newConnection(cc, stream, nil)
			conn.info = connInfo
			conn.cancel = cancel

			h := func(m *protos.SignedRKSyncMessage) {
				logging.Debug("Got message:", m)
				s.msgPublisher.DeMultiplex(&ReceivedMessageImpl{
					conn:                conn,
					lock:                conn,
					SignedRKSyncMessage: m,
					connInfo:            connInfo,
				})
			}
			conn.handler = interceptAcks(h, connInfo.ID, s.pubSub)
			return conn, nil
		}

		logging.Warningf("Authentication failed: %+v", err)
	}
	cc.Close()
	cancel()
	return nil, errors.WithStack(err)
}

// Send sends a message to remote peers
func (s *Server) Send(msg *protos.SignedRKSyncMessage, peers ...*common.NetworkMember) {
	if s.isStopping() || len(peers) == 0 {
		return
	}
	logging.Debug("Entering, sending", msg, "to ", len(peers), "peers")

	for _, peer := range peers {
		go func(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage) {
			s.sendToEndpoint(peer, msg, false)
		}(peer, msg)
	}
}

// SendWithAck sends a message to remote peers, waiting for acknowledgement from minAck of them, or until a certain timeout expires
func (s *Server) SendWithAck(msg *protos.SignedRKSyncMessage, timeout time.Duration, minAck int, peers ...*common.NetworkMember) AggregatedSendResult {
	if len(peers) == 0 {
		return nil
	}

	var err error
	msg.Nonce = util.RandomUInt64()
	msg, err = msg.NoopSign()

	if s.isStopping() || err != nil {
		if err == nil {
			err = errors.New("Server is stopping")
		}
		results := []SendResult{}
		for _, p := range peers {
			results = append(results, SendResult{error: err, NetworkMember: *p})
		}
		return results
	}

	logging.Debug("Entering, sending", msg, "to ", len(peers), "peers")
	sndFunc := func(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage) {
		s.sendToEndpoint(peer, msg, true)
	}

	subscriptions := make(map[string]func() error)
	for _, p := range peers {
		topic := topicForAck(msg.Nonce, p.PKIID)
		sub := s.pubSub.Subscribe(topic, timeout)
		subscriptions[string(p.PKIID)] = func() error {
			msg, err := sub.Listen()
			if err != nil {
				return err
			}
			ackMsg, isAck := msg.(*protos.Acknowledgement)
			if !isAck {
				return errors.Errorf("Received a message of type %s, expected *protos.Acknowledgement", reflect.TypeOf(msg))
			}
			if ackMsg.Error != "" {
				return errors.New(ackMsg.Error)
			}
			return nil
		}
	}

	waitForAck := func(p *common.NetworkMember) error {
		return subscriptions[string(p.PKIID)]()
	}
	ackOperation := newAckSendOperation(sndFunc, waitForAck)
	return ackOperation.send(msg, minAck, peers...)
}

func (s *Server) sendToEndpoint(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage, shouldBlock bool) {
	if s.isStopping() {
		return
	}
	logging.Debug("Entering, Sending to", peer.Endpoint, ", msg", msg)
	defer logging.Debug("Exiting")

	conn, err := s.connStore.getConnection(peer)
	if err == nil {
		disConnectOnErr := func(err error) {
			logging.Warningf("%v isn't responsive: %v", peer.Endpoint, err)
			s.disconnect(peer.PKIID)
		}
		conn.send(msg, disConnectOnErr, shouldBlock)
		return
	}
	logging.Warningf("Failed obtaining connection for %v reason: %v", peer.Endpoint, err)
	s.disconnect(peer.PKIID)
}

// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
func (s *Server) Accept(acceptor common.MessageAcceptor) <-chan protos.ReceivedMessage {
	genericChan := s.msgPublisher.AddChannel(acceptor)
	specificChan := make(chan protos.ReceivedMessage, 10)

	if s.isStopping() {
		logging.Warning("Accept() called but server is stopping, returning empty channel")
		return specificChan
	}

	s.lock.Lock()
	s.subscriptions = append(s.subscriptions, specificChan)
	s.lock.Unlock()

	s.stopWG.Add(1)
	go func() {
		defer logging.Debug("Exiting Accept() loop")
		defer s.stopWG.Done()

		for {
			select {
			case msg := <-genericChan:
				if msg == nil {
					return
				}
				select {
				case specificChan <- msg.(*ReceivedMessageImpl):
				case <-s.exitChan:
					return
				}
			case <-s.exitChan:
				return
			}
		}
	}()

	return specificChan
}

// Handshake authenticates a remote peer and returns
// (its identity, nil) on success and (nil, error)
func (s *Server) Handshake(peer *common.NetworkMember) (common.PeerIdentityType, error) {
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, s.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, defDialTimeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, peer.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	cl := protos.NewRKSyncClient(cc)
	ctx, cancel = context.WithTimeout(context.Background(), defConnTimeout)
	defer cancel()
	if _, err = cl.Ping(ctx, &empty.Empty{}); err != nil {
		return nil, err
	}

	ctx, cancel = context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()
	stream, err := cl.SyncStream(ctx)
	if err != nil {
		return nil, err
	}
	connInfo, err := s.authenticateRemotePeer(stream)
	if err != nil {
		logging.Warningf("Authentication failed: %v", err)
		return nil, err
	}

	if len(peer.PKIID) > 0 && !bytes.Equal(connInfo.ID, peer.PKIID) {
		return nil, errors.New("PKI-ID of remote peer doesn't match expected PKI-ID")
	}
	return connInfo.Identity, nil
}

// Probe probes a remote node and returns nil if its responsive,
// and an error if it's not
func (s *Server) Probe(remotePeer *common.NetworkMember) error {
	var dialOpts []grpc.DialOption
	endpoint := remotePeer.Endpoint
	pkiID := remotePeer.PKIID
	if s.isStopping() {
		return errors.New("Stopping")
	}

	logging.Debug("Entering, endpoint:", endpoint, "PKIID:", pkiID)
	dialOpts = append(dialOpts, s.secureDialOpts()...)
	dialOpts = append(dialOpts, grpc.WithBlock())

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, defDialTimeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, endpoint, dialOpts...)
	if err != nil {
		logging.Debugf("Returning %v", err)
		return err
	}
	defer cc.Close()

	cl := protos.NewRKSyncClient(cc)
	ctx, cancel = context.WithTimeout(context.Background(), defConnTimeout)
	defer cancel()

	_, err = cl.Ping(ctx, &empty.Empty{})
	logging.Debugf("Returning %v", err)
	return err
}

// CloseConn closes a connection to a certain endpoint
func (s *Server) CloseConn(peer *common.NetworkMember) {
	logging.Debug("Closing connection for", peer.Endpoint)
	s.connStore.closeConn(peer)
}

// PresumedDead returns a read-only channel for node endpoints that are suspected to be offline
func (s *Server) PresumedDead() <-chan common.PKIidType {
	return s.deadEndpoints
}

func (s *Server) closeSubscriptions() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, ch := range s.subscriptions {
		close(ch)
	}
}

// Stop stop the server
func (s *Server) Stop() {
	if !atomic.CompareAndSwapInt32(&s.stopping, 0, int32(1)) {
		return
	}
	logging.Info("Stopping")
	defer logging.Info("Stopped")
	if s.gSrv != nil {
		s.gSrv.Stop()
	}
	s.connStore.shutdown()
	logging.Debug("Shut down connection store, connection count:", s.connStore.connNum())
	s.msgPublisher.Close()
	close(s.exitChan)
	s.stopWG.Wait()
	s.closeSubscriptions()
}

// SyncStream is the gRPC stream used for sending and receiving messages
func (s *Server) SyncStream(stream protos.RKSync_SyncStreamServer) error {
	if s.isStopping() {
		return errors.New("Shutting down")
	}
	connInfo, err := s.authenticateRemotePeer(stream)
	if err != nil {
		logging.Errorf("Authentication failed: %v", err)
		return err
	}
	logging.Debug("Servicing", extractRemoteAddress(stream))

	conn := s.connStore.onConnected(stream, connInfo)

	h := func(m *protos.SignedRKSyncMessage) {
		s.msgPublisher.DeMultiplex(&ReceivedMessageImpl{
			conn:                conn,
			lock:                conn,
			SignedRKSyncMessage: m,
			connInfo:            connInfo,
		})
	}
	conn.handler = h

	defer func() {
		logging.Debug("Client", extractRemoteAddress(stream), "disconnected")
		s.connStore.closeByPKIid(connInfo.ID)
		conn.close()
	}()

	return conn.serviceConnection()
}

// Ping is used to probe a remote peer's aliveness
func (s *Server) Ping(context.Context, *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *Server) disconnect(pkiID common.PKIidType) {
	if s.isStopping() {
		return
	}
	s.deadEndpoints <- pkiID
	s.connStore.closeByPKIid(pkiID)
}

func (s *Server) authenticateRemotePeer(stream stream) (*protos.ConnectionInfo, error) {
	remoteAddress := extractRemoteAddress(stream)

	var err error
	var cMsg *protos.SignedRKSyncMessage
	var selfCertHash []byte

	signer := func(msg []byte) ([]byte, error) {
		return s.idMapper.Sign(msg)
	}

	cMsg, err = s.createConnectionMsg(s.pkiID, selfCertHash, s.peerIdentity, signer)
	if err != nil {
		return nil, err
	}

	logging.Debug("Sending", cMsg, "to", remoteAddress)
	stream.Send(cMsg.Envelope)
	m, err := readWithTimeout(stream, defConnTimeout, remoteAddress)
	if err != nil {
		logging.Warningf("Failed reading message from %s, reason %v", remoteAddress, err)
		return nil, err
	}

	receivedMsg := m.GetConn()
	if receivedMsg == nil {
		logging.Warning("Expected connection message from", remoteAddress, "but got", receivedMsg)
		return nil, errors.New("Wrong type")
	}

	if receivedMsg.PkiId == nil {
		logging.Warningf("%s didn't send a pkiID", remoteAddress)
		return nil, errors.New("No PKI-ID")
	}

	logging.Debug("Received", receivedMsg, "from", remoteAddress)
	err = s.idMapper.Put(receivedMsg.PkiId, receivedMsg.Identity)
	if err != nil {
		logging.Warningf("Identity store rejected %s: %v", remoteAddress, err)
		return nil, err
	}

	connInfo := &protos.ConnectionInfo{
		ID:       receivedMsg.PkiId,
		Identity: receivedMsg.Identity,
		Endpoint: remoteAddress,
	}

	verifier := func(peerIdentity []byte, signature, message []byte) error {
		pkiID := s.idMapper.GetPKIidOfCert(common.PeerIdentityType(peerIdentity))
		return s.idMapper.Verify(pkiID, signature, message)
	}

	err = m.Verify(receivedMsg.Identity, verifier)
	if err != nil {
		logging.Errorf("Failed verifying signature from %s: %v", remoteAddress, err)
		return nil, err
	}

	logging.Debug("Authenticated", remoteAddress)
	return connInfo, nil
}

func (s *Server) createConnectionMsg(pkiID common.PKIidType, certHash []byte, cert common.PeerIdentityType, signer protos.Signer) (*protos.SignedRKSyncMessage, error) {
	m := &protos.RKSyncMessage{
		Tag: protos.RKSyncMessage_EMPTY,
		Content: &protos.RKSyncMessage_Conn{
			Conn: &protos.ConnEstablish{
				Identity: cert,
				PkiId:    pkiID,
			},
		},
	}

	sMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: m,
	}
	_, err := sMsg.Sign(signer)
	return sMsg, errors.WithStack(err)
}

func (s *Server) isStopping() bool {
	return atomic.LoadInt32(&s.stopping) == int32(1)
}

func extractRemoteAddress(stream stream) string {
	var remoteAddress string
	p, ok := peer.FromContext(stream.Context())
	if ok {
		if address := p.Addr; address != nil {
			remoteAddress = address.String()
		}
	}
	return remoteAddress
}

func readWithTimeout(stream interface{}, timeout time.Duration, address string) (*protos.SignedRKSyncMessage, error) {
	incChan := make(chan *protos.SignedRKSyncMessage, 1)
	errChan := make(chan error, 1)
	go func() {
		if srvStr, isServerStr := stream.(protos.RKSync_SyncStreamServer); isServerStr {
			if m, err := srvStr.Recv(); err == nil {
				msg, err := m.ToRKSyncMessage()
				if err != nil {
					errChan <- err
					return
				}
				incChan <- msg
			}
		} else if clStr, isClientStr := stream.(protos.RKSync_SyncStreamClient); isClientStr {
			if m, err := clStr.Recv(); err == nil {
				msg, err := m.ToRKSyncMessage()
				if err != nil {
					errChan <- err
					return
				}
				incChan <- msg
			}
		} else {
			panic(errors.Errorf("Stream isn't a SyncStreamServer or a SyncStreamClient, but %v, Aborting", reflect.TypeOf(stream)))
		}
	}()

	select {
	case <-time.After(timeout):
		return nil, errors.Errorf("timed out waiting for connection message from %s", address)
	case m := <-incChan:
		return m, nil
	case err := <-errChan:
		return nil, errors.WithStack(err)
	}
}

func topicForAck(nonce uint64, pkiID common.PKIidType) string {
	return fmt.Sprintf("%d-%s", nonce, hex.EncodeToString(pkiID))
}
