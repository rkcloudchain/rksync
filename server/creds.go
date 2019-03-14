/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/rkcloudchain/rksync/logging"
	"google.golang.org/grpc/credentials"
)

// errors
var (
	ErrClientHandshakeNotImp        = errors.New("server: Client handshake are not implemented with serverCreds")
	ErrOverrideHostnameNotSupported = errors.New("server: OverrideServerName is not supported")
	ErrMissingServerConfig          = errors.New("server: `serverConfig` cannot be nil")
	alpnProtoStr                    = []string{"h2"}
)

// NewServerTransportCredentials returns a new initialized
// grpc/credentials.TransportCredentials
func NewServerTransportCredentials(serverConfig *tls.Config) credentials.TransportCredentials {
	serverConfig.NextProtos = alpnProtoStr
	serverConfig.MinVersion = tls.VersionTLS12
	serverConfig.MaxVersion = tls.VersionTLS12
	return &serverCreds{
		serverConfig: serverConfig,
	}
}

// serverCreds is an implementation of grpc/credentials.TransportCredentials.
type serverCreds struct {
	serverConfig *tls.Config
}

func (sc *serverCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, ErrClientHandshakeNotImp
}

func (sc *serverCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn := tls.Server(rawConn, sc.serverConfig)
	if err := conn.Handshake(); err != nil {
		logging.Errorf("TLS handshake failed with error: %s", err)
		return nil, nil, err
	}

	return conn, credentials.TLSInfo{State: conn.ConnectionState()}, nil
}

func (sc *serverCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
	}
}

func (sc *serverCreds) Clone() credentials.TransportCredentials {
	return NewServerTransportCredentials(sc.serverConfig)
}

func (sc *serverCreds) OverrideServerName(string) error {
	return ErrOverrideHostnameNotSupported
}
