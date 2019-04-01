/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/identity"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/util"
	"google.golang.org/grpc"
)

var (
	inst1 *Server
	inst2 *Server
)

func TestMain(m *testing.M) {
	var srv1 *server.GRPCServer
	var srv2 *server.GRPCServer
	var err error

	home1, err := filepath.Abs("../tests/fixtures/identity/peer0")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	cfg1 := &config.IdentityConfig{
		ID: "peer0.org1",
	}
	err = cfg1.MakeFilesAbs(home1)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	inst1, srv1, err = CreateRPCServer("localhost:9053", cfg1)
	if err != nil {
		panic(err)
	}

	home2, err := filepath.Abs("../tests/fixtures/identity/peer1")
	if err != nil {
		fmt.Printf("Abs failed: %s\n", err)
		os.Exit(-1)
	}

	cfg2 := &config.IdentityConfig{
		ID: "peer1.org2",
	}
	err = cfg2.MakeFilesAbs(home2)
	if err != nil {
		fmt.Printf("MakeFilesAbs failed: %s\n", err)
		os.Exit(-1)
	}

	inst2, srv2, err = CreateRPCServer("localhost:10053", cfg2)
	if err != nil {
		panic(err)
	}

	go srv1.Start()
	defer srv1.Stop()
	go srv2.Start()
	defer srv2.Stop()

	os.Exit(m.Run())
}

// CreateRPCServer create rpc server
func CreateRPCServer(address string, cfg *config.IdentityConfig) (*Server, *server.GRPCServer, error) {
	selfIdentity, err := util.GetIdentity(cfg)
	if err != nil {
		return nil, nil, err
	}

	idMapper, err := identity.NewIdentity(cfg, selfIdentity, func(_ common.PKIidType) {})
	if err != nil {
		return nil, nil, err
	}

	srv, err := server.NewGRPCServer(address, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		return nil, nil, err
	}

	rpcSrv := NewServer(srv.Server(), idMapper, selfIdentity, func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithInsecure()}
	})

	return rpcSrv, srv, nil
}
