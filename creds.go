package rksync

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc/credentials"
)

func clientTransportCredentials(cfg *config.Config) (credentials.TransportCredentials, error) {
	clientCert, err := clientCertificate(cfg)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
	}
	certPool := x509.NewCertPool()
	for _, root := range cfg.Server.SecOpts.ServerRootCAs {
		pemCerts, err := ioutil.ReadFile(root)
		if err != nil {
			logging.Errorf("Failed read root ca file: %s (%s)", root, err)
			return nil, err
		}
		err = addPemToCertPool(pemCerts, certPool)
		if err != nil {
			logging.Errorf("Failed adding certificates to peer's client TLS trust pool: %s", err)
			return nil, err
		}
	}

	tlsConfig.RootCAs = certPool
	return credentials.NewTLS(tlsConfig), nil
}

func addPemToCertPool(pemCerts []byte, pool *x509.CertPool) error {
	certs, _, err := util.PEMToX509Certs(pemCerts)
	if err != nil {
		return err
	}
	for _, cert := range certs {
		pool.AddCert(cert)
	}

	return nil
}

func clientCertificate(cfg *config.Config) (tls.Certificate, error) {
	clientKey, err := ioutil.ReadFile(cfg.Server.SecOpts.Key)
	if err != nil {
		return tls.Certificate{}, errors.WithMessage(err, "error loading client TLS key")
	}

	clientCert, err := ioutil.ReadFile(cfg.Server.SecOpts.Certificate)
	if err != nil {
		return tls.Certificate{}, errors.WithMessage(err, "error loading client TLS certificate")
	}

	cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return tls.Certificate{}, errors.WithMessage(err, "error parsing client TLS key pair")
	}

	return cert, nil
}
