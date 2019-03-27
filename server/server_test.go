/*
Copyright Rockontrol Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"net"
	"testing"

	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
)

func TestNewGRPCServerInvalidParameters(t *testing.T) {
	_, err := NewGRPCServer("", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	_, err = NewGRPCServer("abcdef", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	_, err = NewGRPCServer("localhost:abcd", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create listener [%s]", err)
	}
	defer lis.Close()

	srv, err := NewGRPCServerFromListener(lis, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		t.Fatalf("Failed to create GRPCServer [%s]", err)
	}
	defer srv.Stop()

	_, err = NewGRPCServer(lis.Addr().String(), &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Contains(t, err.Error(), "address already in use")
}

func TestNewGRPCServer(t *testing.T) {
	testAddress := "localhost:9053"
	_, err := NewGRPCServer(testAddress, &config.ServerConfig{
		SecOpts: &config.TLSConfig{
			UseTLS:            true,
			Key:               util.GetTLSPath("server.key"),
			Certificate:       util.GetTLSPath("server.crt"),
			ClientRootCAs:     []string{util.GetTLSPath("ca.crt")},
			RequireClientCert: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to return new GRPC server: %v", err)
	}
}
