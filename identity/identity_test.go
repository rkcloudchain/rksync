/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package identity

import (
	"path/filepath"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/rkcloudchain/cccsp/provider"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	home, err := filepath.Abs("../tests/fixtures/identity/peer0")
	require.NoError(t, err)

	cfg := &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = cfg.MakeFilesAbs(home)
	require.NoError(t, err)

	selfIdentity, err := util.GetIdentity(cfg)
	require.NoError(t, err)

	idMapper, err := NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	assert.NoError(t, err)

	vid := idMapper.GetPKIidOfCert(selfIdentity)
	require.NotNil(t, vid)

	signed, err := idMapper.Sign([]byte("bla bla"))
	assert.NoError(t, err)
	assert.NoError(t, idMapper.Verify(vid, signed, []byte("bla bla")))
}

func TestGet(t *testing.T) {
	home, err := filepath.Abs("../tests/fixtures/identity/peer0")
	require.NoError(t, err)

	cfg := &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = cfg.MakeFilesAbs(home)
	require.NoError(t, err)

	selfIdentity, err := util.GetIdentity(cfg)
	require.NoError(t, err)

	idMapper, err := NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	assert.NoError(t, err)

	vid := idMapper.GetPKIidOfCert(selfIdentity)
	require.NotNil(t, vid)
	assert.NoError(t, idMapper.Put(vid, selfIdentity))
	identity, err := idMapper.Get(vid)
	assert.Nil(t, err)
	assert.Equal(t, selfIdentity, common.PeerIdentityType(identity))
}

func TestGetPKIidOfCert(t *testing.T) {
	idMapper := &identityMapper{csp: provider.GetDefault()}
	id := idMapper.GetPKIidOfCert(nil)
	assert.Nil(t, id)

	id = idMapper.GetPKIidOfCert(common.PeerIdentityType{0, 1, 2, 3, 4})
	assert.Nil(t, id)

	sid := &protos.SerializedIdentity{NodeId: "peer1"}
	bs, _ := proto.Marshal(sid)

	id = idMapper.GetPKIidOfCert(bs)
	assert.NotNil(t, id)
}

func TestNewIdentity(t *testing.T) {
	home, err := filepath.Abs("../tests/fixtures/identity/ca_false")
	require.NoError(t, err)

	cfg := &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = cfg.MakeFilesAbs(home)
	require.NoError(t, err)

	selfIdentity, err := util.GetIdentity(cfg)
	require.NoError(t, err)

	_, err = NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid x.509 certificate")

	home, err = filepath.Abs("../tests/fixtures/identity/cert_false")
	require.NoError(t, err)

	err = cfg.MakeFilesAbs(home)
	require.NoError(t, err)

	selfIdentity, err = util.GetIdentity(cfg)
	require.NoError(t, err)

	_, err = NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "The supplied identity is not valid")
}
