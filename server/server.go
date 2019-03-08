package server

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc"
)

// GRPCServer is the wrapper of grpc.Server
type GRPCServer struct {
	address   string
	listener  net.Listener
	server    *grpc.Server
	tlsConfig *tls.Config
}

// NewGRPCServer creates a new implementation of a GRPCServer given a
// listen address
func NewGRPCServer(address string, serverConfig *config.ServerConfig) (*GRPCServer, error) {
	if address == "" {
		return nil, errors.New("Missing address parameter")
	}

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return NewGRPCServerFromListener(lis, serverConfig)
}

// NewGRPCServerFromListener creates a new instance of GRPCServer given
// on existing net.Listener
func NewGRPCServerFromListener(listener net.Listener, serverConfig *config.ServerConfig) (*GRPCServer, error) {
	grpcServer := &GRPCServer{
		address:  listener.Addr().String(),
		listener: listener,
	}

	var serverOpts []grpc.ServerOption

	var secureConfig config.TLSConfig
	if serverConfig.SecOpts != nil {
		secureConfig = *serverConfig.SecOpts
	}

	if secureConfig.UseTLS {
		if secureConfig.Key != "" && secureConfig.Certificate != "" {
			certPEM, err := ioutil.ReadFile(secureConfig.Certificate)
			if err != nil {
				return nil, err
			}
			keyPEM, err := ioutil.ReadFile(secureConfig.Key)
			if err != nil {
				return nil, err
			}

			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, err
			}

			if len(secureConfig.CipherSuites) == 0 {
				secureConfig.CipherSuites = config.DefaultTLSCipherSuites
			}

			grpcServer.tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				CipherSuites: secureConfig.CipherSuites,
			}
			grpcServer.tlsConfig.ClientAuth = tls.RequestClientCert
			if secureConfig.RequireClientCert {
				grpcServer.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
				if len(secureConfig.ClientRootCAs) > 0 {
					grpcServer.tlsConfig.ClientCAs = x509.NewCertPool()
					for _, clientRootCA := range secureConfig.ClientRootCAs {
						rootCAPEM, err := ioutil.ReadFile(clientRootCA)
						if err != nil {
							return nil, err
						}
						err = grpcServer.appendClientRootCA(rootCAPEM)
						if err != nil {
							return nil, err
						}
					}
				}
			}

			creds := NewServerTransportCredentials(grpcServer.tlsConfig)
			serverOpts = append(serverOpts, grpc.Creds(creds))

		} else {
			return nil, errors.New("serverConfig.SecOpts must contain both Key and Certificate when UseTLS is true")
		}
	}

	serverOpts = append(serverOpts, grpc.MaxSendMsgSize(config.MaxSendMsgSize))
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(config.MaxRecvMsgSize))
	serverOpts = append(serverOpts, config.ServerKeepaliveOptions(serverConfig.KaOpts)...)

	if serverConfig.ConnectionTimeout <= 0 {
		serverConfig.ConnectionTimeout = config.DefaultConnectionTimeout
	}
	serverOpts = append(serverOpts, grpc.ConnectionTimeout(serverConfig.ConnectionTimeout))

	grpcServer.server = grpc.NewServer(serverOpts...)
	return grpcServer, nil
}

// Server returns the grpc.Server for the GRPCServer instance
func (srv *GRPCServer) Server() *grpc.Server {
	return srv.server
}

// Start starts the underlying grpc.Server
func (srv *GRPCServer) Start() error {
	return srv.server.Serve(srv.listener)
}

// Stop stops the underlying grpc.Server
func (srv *GRPCServer) Stop() {
	srv.server.Stop()
}

func (srv *GRPCServer) appendClientRootCA(clientRoot []byte) error {
	errmsg := "Falied to append client root certificate(s): %s"
	certs, _, err := util.PEMToX509Certs(clientRoot)
	if err != nil {
		return errors.Errorf(errmsg, err)
	}

	if len(certs) < 1 {
		return errors.Errorf(errmsg, "No client root certificate found")
	}

	for _, cert := range certs {
		srv.tlsConfig.ClientCAs.AddCert(cert)
	}

	return nil
}
