/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"os"
	"testing"

	"github.com/rkcloudchain/rksync/rpc"
	"github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/runner"
)

var (
	inst1 *rpc.Server
	inst2 *rpc.Server
)

func TestMain(m *testing.M) {
	var srv1 *server.GRPCServer
	var srv2 *server.GRPCServer
	var err error

	inst1, srv1, err = runner.CreateRPCServer("localhost:9053", runner.GetOrg1IdentityConfig())
	if err != nil {
		panic(err)
	}

	inst2, srv2, err = runner.CreateRPCServer("localhost:10053", runner.GetOrg2IdentityConfig())
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
