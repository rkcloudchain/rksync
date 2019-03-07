package identity

import (
	"encoding/pem"
	"io/ioutil"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/tests/util"
	rkutil "github.com/rkcloudchain/rksync/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentityCreate(t *testing.T) {
	certfile := util.GetIdentityPath(t, "signcert.pem")
	keyfile := util.GetIdentityPath(t, "signkey")
	cafile := util.GetIdentityPath(t, "ca.pem")

	cfg := &config.IdentityConfig{
		Certificate: certfile,
		Key:         keyfile,
		CAs:         []string{cafile},
	}

	certBytes, err := ioutil.ReadFile(certfile)
	require.NoError(t, err)

	cert, err := rkutil.GetX509CertificateFromPEM(certBytes)
	require.NoError(t, err)

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	require.NotNil(t, idBytes)

	sid := &protos.SerializedIdentity{NodeId: "peer0.org1", IdBytes: idBytes}
	selfIdentity, err := proto.Marshal(sid)

	_, err = identity.NewIdentity(cfg, selfIdentity)
	assert.NoError(t, err)
}
