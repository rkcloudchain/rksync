/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/rpc"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var idCfg1 *config.IdentityConfig
var idCfg2 *config.IdentityConfig
var idCfg3 *config.IdentityConfig

func TestMain(m *testing.M) {
	home1, err := filepath.Abs("../tests/fixtures/identity/peer0")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	idCfg1 = &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = idCfg1.MakeFilesAbs(home1)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	home2, err := filepath.Abs("../tests/fixtures/identity/peer1")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	idCfg2 = &config.IdentityConfig{
		ID: "peer1.org2",
	}
	err = idCfg2.MakeFilesAbs(home2)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	home3, err := filepath.Abs("../tests/fixtures/identity/peer2")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	idCfg3 = &config.IdentityConfig{
		ID: "peer2.org3",
	}
	err = idCfg3.MakeFilesAbs(home3)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	os.Exit(m.Run())
}

var noopPolicy = func(remotePeer *common.NetworkMember) (Sieve, EnvelopeFilter) {
	return func(msg *protos.SignedRKSyncMessage) bool {
			return true
		}, func(message *protos.SignedRKSyncMessage) *protos.Envelope {
			return message.Envelope
		}
}

func TestConnect(t *testing.T) {

	selfIdentity1, err := util.GetIdentity(idCfg1)
	require.NoError(t, err)
	selfIdentity2, err := util.GetIdentity(idCfg2)
	require.NoError(t, err)

	idMapper1, err := identity.NewIdentity(idCfg1, selfIdentity1, func(_ common.PKIidType) {})
	require.NoError(t, err)
	idMapper2, err := identity.NewIdentity(idCfg2, selfIdentity2, func(_ common.PKIidType) {})
	require.NoError(t, err)

	rpc1, grpc1, err := CreateRPCServerWithIdentity("localhost:9053", selfIdentity1, idMapper1)
	require.NoError(t, err)
	go grpc1.Start()
	defer rpc1.Stop()

	rpc2, grpc2, err := CreateRPCServerWithIdentity("localhost:10053", selfIdentity2, idMapper2)
	require.NoError(t, err)
	go grpc2.Start()
	defer rpc2.Stop()

	self1 := common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()}
	self2 := common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()}

	mockRPC1 := &mockRPCService{
		rpc: rpc1,
		membership: func() []common.NetworkMember {
			return []common.NetworkMember{
				common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()},
			}
		},
	}

	mockRPC2 := &mockRPCService{
		rpc: rpc2,
		membership: func() []common.NetworkMember {
			return []common.NetworkMember{
				common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()},
			}
		},
	}
	disc1 := NewDiscoveryService(self1, mockRPC1, &mockCryptoService{idMapper1}, noopPolicy)
	disc2 := NewDiscoveryService(self2, mockRPC2, &mockCryptoService{idMapper2}, noopPolicy)
	defer disc1.Stop()
	defer disc2.Stop()

	identifier := func() (common.PKIidType, error) {
		remote, err := rpc2.Handshake(&common.NetworkMember{Endpoint: "localhost:9053"})
		if err != nil {
			return nil, err
		}
		pki := idMapper2.GetPKIidOfCert(remote)
		if len(pki) == 0 {
			return nil, errors.New("Wasn't able to extract PKI-ID of remote peer with identity")
		}
		return pki, nil
	}

	disc2.Connect(common.NetworkMember{Endpoint: "localhost:9053"}, identifier)

	time.Sleep(5 * time.Second)
	members := disc1.GetMembership()
	fmt.Printf("Members: %+v\n", members)
	assert.Equal(t, 1, len(members))

}

func TestDisconnect(t *testing.T) {
	selfIdentity1, err := util.GetIdentity(idCfg1)
	require.NoError(t, err)
	selfIdentity2, err := util.GetIdentity(idCfg2)
	require.NoError(t, err)
	selfIdentity3, err := util.GetIdentity(idCfg3)
	require.NoError(t, err)

	idMapper1, err := identity.NewIdentity(idCfg1, selfIdentity1, func(_ common.PKIidType) {})
	require.NoError(t, err)
	idMapper2, err := identity.NewIdentity(idCfg2, selfIdentity2, func(_ common.PKIidType) {})
	require.NoError(t, err)
	idMapper3, err := identity.NewIdentity(idCfg3, selfIdentity3, func(_ common.PKIidType) {})
	require.NoError(t, err)

	rpc1, grpc1, err := CreateRPCServerWithIdentity("localhost:9053", selfIdentity1, idMapper1)
	require.NoError(t, err)
	go grpc1.Start()
	defer rpc1.Stop()

	rpc2, grpc2, err := CreateRPCServerWithIdentity("localhost:10053", selfIdentity2, idMapper2)
	require.NoError(t, err)
	go grpc2.Start()
	defer rpc2.Stop()

	rpc3, grcp3, err := CreateRPCServerWithIdentity("localhost:8053", selfIdentity3, idMapper3)
	require.NoError(t, err)
	go grcp3.Start()

	self1 := common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()}
	self2 := common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()}
	self3 := common.NetworkMember{Endpoint: "localhost:8053", PKIID: rpc3.GetPKIid()}

	fmt.Printf("Disc 1: %s\n", rpc1.GetPKIid())
	fmt.Printf("Disc 2: %s\n", rpc2.GetPKIid())
	fmt.Printf("Disc 3: %s\n", rpc3.GetPKIid())

	mockRPC1 := &mockRPCService{
		rpc: rpc1,
		membership: func() []common.NetworkMember {
			return []common.NetworkMember{
				common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:8053", PKIID: rpc3.GetPKIid()},
			}
		},
	}

	mockRPC2 := &mockRPCService{
		rpc: rpc2,
		membership: func() []common.NetworkMember {
			return []common.NetworkMember{
				common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:8053", PKIID: rpc3.GetPKIid()},
			}
		},
	}

	mockRPC3 := &mockRPCService{
		rpc: rpc3,
		membership: func() []common.NetworkMember {
			return []common.NetworkMember{
				common.NetworkMember{Endpoint: "localhost:9053", PKIID: rpc1.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:10053", PKIID: rpc2.GetPKIid()},
				common.NetworkMember{Endpoint: "localhost:8053", PKIID: rpc3.GetPKIid()},
			}
		},
	}
	disc1 := NewDiscoveryService(self1, mockRPC1, &mockCryptoService{idMapper1}, noopPolicy)
	disc2 := NewDiscoveryService(self2, mockRPC2, &mockCryptoService{idMapper2}, noopPolicy)
	disc3 := NewDiscoveryService(self3, mockRPC3, &mockCryptoService{idMapper3}, noopPolicy)
	defer disc1.Stop()
	defer disc2.Stop()
	defer disc3.Stop()

	identifier2 := func() (common.PKIidType, error) {
		remote, err := rpc2.Handshake(&common.NetworkMember{Endpoint: "localhost:9053"})
		if err != nil {
			return nil, err
		}
		pki := idMapper2.GetPKIidOfCert(remote)
		if len(pki) == 0 {
			return nil, errors.New("Wasn't able to extract PKI-ID of remote peer with identity")
		}
		return pki, nil
	}

	identifier3 := func() (common.PKIidType, error) {
		remote, err := rpc3.Handshake(&common.NetworkMember{Endpoint: "localhost:9053"})
		if err != nil {
			return nil, err
		}
		pki := idMapper3.GetPKIidOfCert(remote)
		if len(pki) == 0 {
			return nil, errors.New("Wasn't able to extract PKI-ID of remote peer with identity")
		}
		return pki, nil
	}

	disc2.Connect(common.NetworkMember{Endpoint: "localhost:9053"}, identifier2)
	disc3.Connect(common.NetworkMember{Endpoint: "localhost:9053"}, identifier3)

	time.Sleep(3 * time.Second)
	assert.Len(t, disc1.GetMembership(), 2)

	rpc3.Stop()
	time.Sleep(10 * time.Second)
	assert.Len(t, disc1.GetMembership(), 1)
}

type mockRPCService struct {
	rpc        *rpc.Server
	membership func() []common.NetworkMember
}

func (m *mockRPCService) Gossip(msg *protos.SignedRKSyncMessage) {
	peers2Send := filter.SelectPeers(3, m.membership(), filter.SelectAllPolicy)
	m.rpc.Send(msg, peers2Send...)
}

func (m *mockRPCService) SendToPeer(peer *common.NetworkMember, msg *protos.SignedRKSyncMessage) {
	m.rpc.Send(msg, peer)
}

func (m *mockRPCService) Ping(peer *common.NetworkMember) bool {
	err := m.rpc.Probe(peer)
	return err == nil
}

func (m *mockRPCService) Accept() <-chan protos.ReceivedMessage {
	return m.rpc.Accept(func(msg interface{}) bool {
		return true
	})
}

func (m *mockRPCService) PresumedDead() <-chan common.PKIidType {
	return m.rpc.PresumedDead()
}

func (m *mockRPCService) CloseConn(peer *common.NetworkMember) {
	m.rpc.CloseConn(peer)
}

func (m *mockRPCService) Forward(msg protos.ReceivedMessage) {
	peers2Send := filter.SelectPeers(3, m.membership(), func(member common.NetworkMember) bool {
		return msg.GetConnectionInfo().ID.IsNotSameFilter(member.PKIID)
	})
	m.rpc.Send(msg.GetRKSyncMessage(), peers2Send...)
}

type mockCryptoService struct {
	idMapper identity.Identity
}

func (m *mockCryptoService) SignMessage(message *protos.RKSyncMessage) *protos.Envelope {
	signer := func(msg []byte) ([]byte, error) {
		return m.idMapper.Sign(msg)
	}

	signedMsg := &protos.SignedRKSyncMessage{
		RKSyncMessage: message,
	}
	envp, err := signedMsg.Sign(signer)
	if err != nil {
		return nil
	}
	return envp
}

func (m *mockCryptoService) ValidateAliveMsg(message *protos.SignedRKSyncMessage) bool {
	am := message.GetAliveMsg()
	if am == nil || am.Membership == nil || am.Membership.PkiId == nil || !message.IsSigned() {
		return false
	}

	if am.Identity != nil {
		identity := common.PeerIdentityType(am.Identity)
		claimedPKIID := am.Membership.PkiId
		err := m.idMapper.Put(claimedPKIID, identity)
		if err != nil {
			return false
		}
	} else {
		cert, err := m.idMapper.Get(am.Membership.PkiId)
		if err != nil {
			fmt.Printf("Get member certificate for %s error: %s\n", common.PKIidType(am.Membership.PkiId), err)
			return false
		}
		if cert == nil {
			return false
		}
	}

	verifier := func(pkiID []byte, signature, message []byte) error {
		return m.idMapper.Verify(common.PKIidType(pkiID), signature, message)
	}

	err := message.Verify(am.Membership.PkiId, verifier)
	if err != nil {
		return false
	}
	return true
}

// CreateRPCServerWithIdentity creates rpc server
func CreateRPCServerWithIdentity(address string, selfIdentity common.PeerIdentityType, idMapper identity.Identity) (*rpc.Server, *server.GRPCServer, error) {
	srv, err := server.NewGRPCServer(address, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		return nil, nil, err
	}

	rpcSrv := rpc.NewServer(srv.Server(), idMapper, selfIdentity, func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithInsecure()}
	})

	return rpcSrv, srv, nil
}
