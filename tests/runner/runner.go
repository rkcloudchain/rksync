package runner

import (
	"encoding/pem"
	"errors"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/rpc"
	"github.com/rkcloudchain/rksync/server"
	rkserver "github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/util"
	rkutil "github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
)

// GetIdentity gets peer identity
func GetIdentity(cfg *config.IdentityConfig) (common.PeerIdentityType, error) {
	certBytes, err := ioutil.ReadFile(cfg.Certificate)
	if err != nil {
		return nil, err
	}

	cert, err := rkutil.GetX509CertificateFromPEM(certBytes)
	if err != nil {
		return nil, err
	}

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	if idBytes == nil {
		return nil, errors.New("Encoding of identity failed")
	}

	sid := &protos.SerializedIdentity{NodeId: cfg.ID, IdBytes: idBytes}
	selfIdentity, err := proto.Marshal(sid)
	if err != nil {
		return nil, err
	}

	return selfIdentity, nil
}

// GetOrg1IdentityConfig gets org1 identity cfg
func GetOrg1IdentityConfig() *config.IdentityConfig {
	return &config.IdentityConfig{
		ID:          "peer0.org1",
		Certificate: util.GetIdentityPath("signcert.org1.pem"),
		Key:         util.GetIdentityPath("signkey.org1"),
		CAs:         []string{util.GetIdentityPath("ca.org1.pem"), util.GetIdentityPath("ca.org2.pem")},
	}
}

// GetOrg2IdentityConfig gets org2 identity cfg
func GetOrg2IdentityConfig() *config.IdentityConfig {
	return &config.IdentityConfig{
		ID:          "peer0.org2",
		Certificate: util.GetIdentityPath("signcert.org2.pem"),
		Key:         util.GetIdentityPath("signkey.org2"),
		CAs:         []string{util.GetIdentityPath("ca.org1.pem"), util.GetIdentityPath("ca.org2.pem")},
	}
}

// CreateRPCServer create rpc server
func CreateRPCServer(address string, cfg *config.IdentityConfig) (*rpc.Server, *server.GRPCServer, error) {
	selfIdentity, err := GetIdentity(cfg)
	if err != nil {
		return nil, nil, err
	}

	idMapper, err := identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		return nil, nil, err
	}

	srv, err := rkserver.NewGRPCServer(address, &config.ServerConfig{
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

// CreateRPCServerWithIdentity creates rpc server
func CreateRPCServerWithIdentity(address string, selfIdentity common.PeerIdentityType, idMapper identity.Identity) (*rpc.Server, *server.GRPCServer, error) {
	srv, err := rkserver.NewGRPCServer(address, &config.ServerConfig{
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
