package identity

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
	"github.com/rkcloudchain/rksync/tests/util"
	rkutil "github.com/rkcloudchain/rksync/util"
)

var idMapper identity.Identity
var selfIdentity common.PeerIdentityType

func TestMain(m *testing.M) {
	certfile := util.GetIdentityPath("signcert.org1.pem")
	keyfile := util.GetIdentityPath("signkey.org1")
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
	selfIdentity, err = proto.Marshal(sid)
	if err != nil {
		panic(err)
	}

	idMapper, err = identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		panic(err)
	}

	gr := m.Run()
	os.Exit(gr)
}
