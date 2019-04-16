/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"fmt"
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

func TestConnect(t *testing.T) {
	disc1, rpc1, err := CreateDiscoveryInstance("localhost:9053", 0)
	require.NoError(t, err)
	defer disc1.Stop()
	defer rpc1.Stop()

	disc2, rpc2, err := CreateDiscoveryInstance("localhost:9054", 1)
	require.NoError(t, err)
	defer disc2.Stop()
	defer rpc2.Stop()

	disc2.Connect(common.NetworkMember{Endpoint: "localhost:9053"}, func() (common.PKIidType, error) {
		return rpc1.GetPKIid(), nil
	})

	time.Sleep(5 * time.Second)
	members := disc1.GetMembership()
	fmt.Printf("Members: %+v\n", members)
	assert.Equal(t, 1, len(members))

}

func TestDisconnect(t *testing.T) {
	disc1, rpc1, err := CreateDiscoveryInstance("localhost:8053", 0)
	require.NoError(t, err)
	defer disc1.Stop()
	defer rpc1.Stop()

	disc2, rpc2, err := CreateDiscoveryInstance("localhost:8054", 1)
	require.NoError(t, err)
	defer disc2.Stop()
	defer rpc2.Stop()

	disc3, rpc3, err := CreateDiscoveryInstance("localhost:8055", 2)
	require.NoError(t, err)
	defer disc3.Stop()
	defer rpc3.Stop()

	disc2.Connect(common.NetworkMember{Endpoint: "localhost:8053"}, func() (common.PKIidType, error) {
		return rpc1.GetPKIid(), nil
	})
	disc3.Connect(common.NetworkMember{Endpoint: "localhost:8053"}, func() (common.PKIidType, error) {
		return rpc1.GetPKIid(), nil
	})

	time.Sleep(3 * time.Second)
	assert.Len(t, disc1.GetMembership(), 2)

	rpc3.Stop()
	time.Sleep(10 * time.Second)
	assert.Len(t, disc1.GetMembership(), 1)
}

func TestLookup(t *testing.T) {
	disc1, rpc1, err := CreateDiscoveryInstance("localhost:7053", 0)
	require.NoError(t, err)
	defer disc1.Stop()
	defer rpc1.Stop()

	disc2, rpc2, err := CreateDiscoveryInstance("localhost:7054", 1)
	require.NoError(t, err)
	defer disc2.Stop()
	defer rpc2.Stop()

	disc2.Connect(common.NetworkMember{Endpoint: "localhost:7053"}, func() (common.PKIidType, error) {
		return rpc1.GetPKIid(), nil
	})

	time.Sleep(5 * time.Second)
	assert.Len(t, disc2.GetMembership(), 1)

	member := disc2.Lookup(rpc1.GetPKIid())
	assert.NotNil(t, member)
	assert.Equal(t, rpc1.GetPKIid(), member.PKIID)
}

func TestMembership(t *testing.T) {
	disc1, rpc1, err := CreateDiscoveryInstance("localhost:4053", 0)
	require.NoError(t, err)
	defer disc1.Stop()
	defer rpc1.Stop()

	disc2, rpc2, err := CreateDiscoveryInstance("localhost:4054", 1)
	require.NoError(t, err)
	defer disc2.Stop()
	defer rpc2.Stop()

	disc3, rpc3, err := CreateDiscoveryInstance("localhost:4055", 2)
	require.NoError(t, err)
	defer disc3.Stop()
	defer rpc3.Stop()

	disc4, rpc4, err := CreateDiscoveryInstance("localhost:4056", 3)
	require.NoError(t, err)
	defer disc4.Stop()
	defer rpc4.Stop()

	disc5, rpc5, err := CreateDiscoveryInstance("localhost:4057", 4)
	require.NoError(t, err)
	defer disc5.Stop()
	defer rpc5.Stop()

	disc6, rpc6, err := CreateDiscoveryInstance("localhost:4058", 5)
	require.NoError(t, err)
	defer disc6.Stop()
	defer rpc6.Stop()

	disc7, rpc7, err := CreateDiscoveryInstance("localhost:4059", 6)
	require.NoError(t, err)
	defer disc7.Stop()
	defer rpc7.Stop()

	disc8, rpc8, err := CreateDiscoveryInstance("localhost:4060", 7)
	require.NoError(t, err)
	defer disc8.Stop()
	defer rpc8.Stop()

	disc9, rpc9, err := CreateDiscoveryInstance("localhost:4061", 8)
	require.NoError(t, err)
	defer disc9.Stop()
	defer rpc9.Stop()

	disc10, rpc10, err := CreateDiscoveryInstance("localhost:4062", 9)
	require.NoError(t, err)
	defer disc10.Stop()
	defer rpc10.Stop()

	go disc2.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc3.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc4.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc5.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc6.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc7.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc8.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc9.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })
	go disc10.Connect(common.NetworkMember{Endpoint: "localhost:4053"}, func() (common.PKIidType, error) { return rpc1.GetPKIid(), nil })

	time.Sleep(15 * time.Second)
	assert.Len(t, disc1.GetMembership(), 9)
	assert.Len(t, disc2.GetMembership(), 9)
	assert.Len(t, disc3.GetMembership(), 9)
	assert.Len(t, disc4.GetMembership(), 9)
	assert.Len(t, disc5.GetMembership(), 9)
	assert.Len(t, disc6.GetMembership(), 9)
	assert.Len(t, disc7.GetMembership(), 9)
	assert.Len(t, disc8.GetMembership(), 9)
	assert.Len(t, disc9.GetMembership(), 9)
	assert.Len(t, disc10.GetMembership(), 9)
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
	identity common.PeerIdentityType
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

func (m *mockCryptoService) SelfIdentity() common.PeerIdentityType {
	return m.identity
}

// CreateRPCServerWithIdentity creates rpc server
func CreateRPCServerWithIdentity(address string, selfIdentity common.PeerIdentityType, idMapper identity.Identity) (*rpc.Server, error) {
	srv, err := server.NewGRPCServer(address, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		return nil, err
	}

	rpcSrv := rpc.NewServer(srv.Server(), idMapper, selfIdentity, func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithInsecure()}
	})
	go srv.Start()

	return rpcSrv, nil
}

// CreateDiscoveryInstance creates discovery instance
func CreateDiscoveryInstance(address string, num int) (Discovery, *rpc.Server, error) {
	home, err := filepath.Abs(fmt.Sprintf("../tests/fixtures/identity/peer%d", num%3))
	if err != nil {
		return nil, nil, err
	}
	idcfg := &config.IdentityConfig{ID: fmt.Sprintf("peer%d.org%d", num, num+1)}
	err = idcfg.MakeFilesAbs(home)
	if err != nil {
		return nil, nil, err
	}

	selfIdentity, err := util.GetIdentity(idcfg)
	if err != nil {
		return nil, nil, err
	}

	idMapper, err := identity.NewIdentity(idcfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		return nil, nil, err
	}

	rpc, err := CreateRPCServerWithIdentity(address, selfIdentity, idMapper)
	if err != nil {
		return nil, nil, err
	}

	mockRPC := &mockRPCService{rpc: rpc}
	disc := NewDiscoveryService(common.NetworkMember{Endpoint: address, PKIID: rpc.GetPKIid()}, mockRPC, &mockCryptoService{idMapper, selfIdentity})
	mockRPC.membership = disc.GetMembership

	return disc, rpc, nil
}
