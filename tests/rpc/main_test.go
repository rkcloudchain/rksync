package rpc

import (
	"encoding/pem"
	"errors"
	"io/ioutil"
	"os"
	"testing"

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

var (
	inst1 *rpc.Server
	inst2 *rpc.Server
)

func TestMain(m *testing.M) {
	certfile1 := util.GetIdentityPath("signcert.org1.pem")
	keyfile1 := util.GetIdentityPath("signkey.org1")
	cafile1 := util.GetIdentityPath("ca.org1.pem")
	certfile2 := util.GetIdentityPath("signcert.org2.pem")
	keyfile2 := util.GetIdentityPath("signkey.org2")
	cafile2 := util.GetIdentityPath("ca.org2.pem")

	cfg1 := &config.IdentityConfig{
		Certificate: certfile1,
		Key:         keyfile1,
		CAs:         []string{cafile1, cafile2},
	}

	var srv1 *server.GRPCServer
	var srv2 *server.GRPCServer
	var err error

	inst1, srv1, err = createRPCServer("localhost:9053", cfg1)
	if err != nil {
		panic(err)
	}

	cfg2 := &config.IdentityConfig{
		Certificate: certfile2,
		Key:         keyfile2,
		CAs:         []string{cafile1, cafile2},
	}
	inst2, srv2, err = createRPCServer("localhost:10053", cfg2)
	if err != nil {
		panic(err)
	}

	go srv1.Start()
	go srv2.Start()

	gr := m.Run()
	srv1.Stop()
	srv2.Stop()
	os.Exit(gr)
}

func createRPCServer(address string, cfg *config.IdentityConfig) (*rpc.Server, *server.GRPCServer, error) {
	certBytes, err := ioutil.ReadFile(cfg.Certificate)
	if err != nil {
		return nil, nil, err
	}

	cert, err := rkutil.GetX509CertificateFromPEM(certBytes)
	if err != nil {
		return nil, nil, err
	}

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	if idBytes == nil {
		return nil, nil, errors.New("Encoding of identity failed")
	}

	sid := &protos.SerializedIdentity{NodeId: "peer0.org1", IdBytes: idBytes}
	selfIdentity, err := proto.Marshal(sid)
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
