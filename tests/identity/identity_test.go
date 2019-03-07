package identity

import (
	"encoding/pem"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	vid := idMapper.GetPKIidOfCert(selfIdentity)
	require.NotNil(t, vid)

	signed, err := idMapper.Sign([]byte("bla bla"))
	assert.NoError(t, err)
	assert.NoError(t, idMapper.Verify(vid, signed, []byte("bla bla")))
}

func TestGet(t *testing.T) {
	vid := idMapper.GetPKIidOfCert(selfIdentity)
	require.NotNil(t, vid)
	assert.NoError(t, idMapper.Put(vid, selfIdentity))
	cert, err := idMapper.Get(vid)
	assert.Nil(t, err)

	block := &pem.Block{Bytes: cert.Raw, Type: "CERTIFICATE"}
	idBytes := pem.EncodeToMemory(block)
	require.NotNil(t, idBytes)

	sid := &protos.SerializedIdentity{NodeId: "peer0.org1", IdBytes: idBytes}
	ident, err := proto.Marshal(sid)
	require.Nil(t, err)
	assert.Equal(t, selfIdentity, common.PeerIdentityType(ident))
}

func TestErrorCA(t *testing.T) {
	certfile := util.GetIdentityPath("signcert.pem")
	keyfile := util.GetIdentityPath("signkey")
	cafile := util.GetIdentityPath("ca.org2.pem")

	cfg := &config.IdentityConfig{
		Certificate: certfile,
		Key:         keyfile,
		CAs:         []string{cafile},
	}

	_, err := identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	assert.NotNil(t, err)
}
