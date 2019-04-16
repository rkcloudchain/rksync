/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package identity

import (
	"path/filepath"
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
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
