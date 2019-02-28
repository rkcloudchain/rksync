package rpc

import (
	"context"

	"github.com/rkcloudchain/rksync/util"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func certHashFromRawCert(rawCert []byte) []byte {
	if len(rawCert) == 0 {
		return nil
	}
	return util.ComputeSHA256(rawCert)
}

func extractCertificateHashFromContext(ctx context.Context) []byte {
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}

	tlsInfo, isTLSConn := authInfo.(credentials.TLSInfo)
	if !isTLSConn {
		return nil
	}

	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return nil
	}

	raw := certs[0].Raw
	return certHashFromRawCert(raw)
}
