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
	certfile := util.GetIdentityPath("signcert.pem")
	keyfile := util.GetIdentityPath("signkey")
	cafile := util.GetIdentityPath("ca.org1.pem")

	cfg := &config.IdentityConfig{
		Certificate: certfile,
		Key:         keyfile,
		CAs:         []string{cafile},
	}

	certBytes, err := ioutil.ReadFile(certfile)
	if err != nil {
		panic(err)
	}

	cert, err := rkutil.GetX509CertificateFromPEM(certBytes)
	if err != nil {
		panic(err)
	}

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	if idBytes == nil {
		panic(errors.New("Encoding of identity failed"))
	}

	sid := &protos.SerializedIdentity{NodeId: "peer0.org1", IdBytes: idBytes}
	selfIdentity, err := proto.Marshal(sid)
	if err != nil {
		panic(err)
	}

	idMapper, err := identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		panic(err)
	}

	srv1, err := rkserver.NewGRPCServer("localhost:9053", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		panic(err)
	}

	srv2, err := rkserver.NewGRPCServer("localhost:10053", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		panic(err)
	}

	inst1 = rpc.NewServer(srv1.Server(), idMapper, selfIdentity, func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithInsecure()}
	})
	inst2 = rpc.NewServer(srv2.Server(), idMapper, selfIdentity, func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithInsecure()}
	})
	go srv1.Start()
	go srv2.Start()

	gr := m.Run()
	srv1.Stop()
	srv2.Stop()
	os.Exit(gr)
}
