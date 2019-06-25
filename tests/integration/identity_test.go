package integration

import (
	"path/filepath"
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/tests/util"
)

func TestLoadBccspIdentity(t *testing.T) {
	home, err := filepath.Abs("../fixtures/identity/certs")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &config.IdentityConfig{ID: "certs"}
	err = cfg.MakeFilesAbs(home)
	if err != nil {
		t.Fatal(err)
	}

	selfIdentity, err := util.GetIdentity(cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		t.Fatal(err)
	}
}
