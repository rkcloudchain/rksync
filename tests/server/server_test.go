package server

import (
	"net"
	"testing"

	"github.com/rkcloudchain/rksync/config"
	rkserver "github.com/rkcloudchain/rksync/server"
	"github.com/rkcloudchain/rksync/tests/util"
	"github.com/stretchr/testify/assert"
)

func TestNewGRPCServerInvalidParameters(t *testing.T) {
	_, err := rkserver.NewGRPCServer("", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	_, err = rkserver.NewGRPCServer("abcdef", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	_, err = rkserver.NewGRPCServer("localhost:abcd", &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Error(t, err)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create listener [%s]", err)
	}
	defer lis.Close()

	srv, err := rkserver.NewGRPCServerFromListener(lis, &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	if err != nil {
		t.Fatalf("Failed to create GRPCServer [%s]", err)
	}
	defer srv.Stop()

	_, err = rkserver.NewGRPCServer(lis.Addr().String(), &config.ServerConfig{
		SecOpts: &config.TLSConfig{UseTLS: false},
	})
	assert.Contains(t, err.Error(), "address already in use")
}

func TestNewGRPCServer(t *testing.T) {
	testAddress := "localhost:9053"
	_, err := rkserver.NewGRPCServer(testAddress, &config.ServerConfig{
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
