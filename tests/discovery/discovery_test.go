/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/discovery"
	"github.com/rkcloudchain/rksync/filter"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/rpc"
	"github.com/rkcloudchain/rksync/tests/runner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var noopPolicy = func(remotePeer *common.NetworkMember) (discovery.Sieve, discovery.EnvelopeFilter) {
	return func(msg *protos.SignedRKSyncMessage) bool {
			return true
		}, func(message *protos.SignedRKSyncMessage) *protos.Envelope {
			return message.Envelope
		}
}

func TestConnect(t *testing.T) {
	cfg1 := runner.GetOrg1IdentityConfig()
	cfg2 := runner.GetOrg2IdentityConfig()

	selfIdentity1, err := runner.GetIdentity(cfg1)
	require.NoError(t, err)
	selfIdentity2, err := runner.GetIdentity(cfg2)
	require.NoError(t, err)

	idMapper1, err := identity.NewIdentity(cfg1, selfIdentity1, func(_ common.PKIidType) {})
	require.NoError(t, err)
	idMapper2, err := identity.NewIdentity(cfg2, selfIdentity2, func(_ common.PKIidType) {})
	require.NoError(t, err)

	rpc1, grpc1, err := runner.CreateRPCServerWithIdentity("localhost:9053", selfIdentity1, idMapper1)
	require.NoError(t, err)
	go grpc1.Start()
	defer rpc1.Stop()

	rpc2, grpc2, err := runner.CreateRPCServerWithIdentity("localhost:10053", selfIdentity2, idMapper2)
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
	disc1 := discovery.NewDiscoveryService(self1, mockRPC1, &mockCryptoService{idMapper1}, noopPolicy)
	disc2 := discovery.NewDiscoveryService(self2, mockRPC2, &mockCryptoService{idMapper2}, noopPolicy)
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
			fmt.Printf("Get member certificate error: %s\n", err)
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
