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

func TestSelf(t *testing.T) {
	disc, rpc, err := CreateDiscoveryInstance("localhost:11111", 0)
	require.NoError(t, err)
	defer disc.Stop()

	srv, ok := disc.(*gossipDiscoveryService)
	assert.True(t, ok)

	time.Sleep(5 * time.Second)

	srv.lock.RLock()
	self := srv.self
	srv.lock.RUnlock()

	assert.Equal(t, "localhost:11111", self.Endpoint)
	assert.Equal(t, rpc.GetPKIid(), self.PKIID)
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
	home, err := filepath.Abs(fmt.Sprintf("../tests/fixtures/identity/peer%d", num))
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
	disc := NewDiscoveryService(common.NetworkMember{Endpoint: address, PKIID: rpc.GetPKIid()}, mockRPC, &mockCryptoService{idMapper})
	mockRPC.membership = disc.GetMembership

	return disc, rpc, nil
}
