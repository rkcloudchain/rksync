package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	defDialTimeout  = time.Second * time.Duration(3)
	defConnTimeout  = time.Second * time.Duration(2)
	defRecvBuffSize = 20
	defSendBuffSize = 20
)

// Server is an object that enables to communicate with other peers
type Server struct {
	secureDialOpts func() []grpc.DialOption
	tlsCert        tls.Certificate
	gSrv           *grpc.Server
	lsnr           net.Listener
	connStore      *connectionStore
	idMapper       identity.Identity
	peerIdentity   common.PeerIdentityType
	pkiID          common.PKIidType
	lock           *sync.Mutex
	stopping       int32
	useTLS         bool
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
			conn := newConnection(cc, stream, nil)
			conn.info = connInfo
			conn.cancel = cancel

			h := func(m *protos.SignedRKSyncMessage) {
				logging.Debug("Got message:", m)

				// TODO: Channel DeMultiplexer

			}
			conn.handler = interceptAcks(h, connInfo.ID)
			return conn, nil
		}

		logging.Warningf("Authentication failed: %+v", err)
	}
	cc.Close()
	cancel()
	return nil, errors.WithStack(err)
}

// Probe probes a remote node and returns nil if its responsive,
// and an error if it's not
func (s *Server) Probe(remotePeer *common.RemotePeer) error {
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

func (s *Server) authenticateRemotePeer(stream stream) (*protos.ConnectionInfo, error) {
	ctx := stream.Context()
	remoteAddress := extractRemoteAddress(stream)
	remoteCertHash := extractCertificateHashFromContext(ctx)

	var err error
	var cMsg *protos.SignedRKSyncMessage
	var selfCertHash []byte

	if s.useTLS {
		selfCertHash = certHashFromRawCert(s.tlsCert.Certificate[0])
	}

	if s.useTLS && len(remoteCertHash) == 0 {
		logging.Warningf("%s didn't send TLS certificate", remoteAddress)
		return nil, errors.Errorf("No TLS certificate")
	}

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

	if s.useTLS {
		if !bytes.Equal(remoteCertHash, receivedMsg.TlsCertHash) {
			return nil, errors.Errorf("Expected %v in remote hash of TLS cert, but got %v", remoteCertHash, receivedMsg.TlsCertHash)
		}
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
				Identity:    cert,
				PkiId:       pkiID,
				TlsCertHash: certHash,
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
