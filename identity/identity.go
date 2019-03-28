/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package identity

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"io/ioutil"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rkcloudchain/cccsp"
	"github.com/rkcloudchain/cccsp/hash"
	"github.com/rkcloudchain/cccsp/importer"
	"github.com/rkcloudchain/cccsp/provider"
	"github.com/rkcloudchain/rksync/common"
	"github.com/rkcloudchain/rksync/config"
	"github.com/rkcloudchain/rksync/logging"
	"github.com/rkcloudchain/rksync/protos"
	"github.com/rkcloudchain/rksync/util"
)

// Identity holds identities of peer
type Identity interface {
	Put(pkiID common.PKIidType, identity common.PeerIdentityType) error
	Get(pkiID common.PKIidType) (*x509.Certificate, error)
	Sign(msg []byte) ([]byte, error)
	Verify(vkID common.PKIidType, signature, message []byte) error
	GetPKIidOfCert(common.PeerIdentityType) common.PKIidType
}

type purgeTrigger func(pkiID common.PKIidType)

type identityMapper struct {
	onPurge           purgeTrigger
	certs             map[string]*storedIdentity
	opts              *x509.VerifyOptions
	rootCerts         []*x509.Certificate
	intermediateCerts []*x509.Certificate
	csp               cccsp.CCCSP
	signer            crypto.Signer
	sync.RWMutex
}

// NewIdentity returns a new Identity instance
func NewIdentity(cfg *config.IdentityConfig, selfIdentity common.PeerIdentityType, onPurge purgeTrigger) (Identity, error) {
	if cfg == nil {
		return nil, errors.New("NewIdentity error: nil cfg reference")
	}

	logging.Debug("Creating Identity instance")
	identity := &identityMapper{
		certs: make(map[string]*storedIdentity),
	}

	keyStoreDir := cfg.GetKeyStoreDir()
	fks, err := provider.NewFileKeyStore(keyStoreDir)
	if err != nil {
		return nil, err
	}
	identity.csp = provider.New(fks)

	selfPKIID := identity.GetPKIidOfCert(selfIdentity)

	if err := identity.setupCAs(cfg); err != nil {
		return nil, err
	}

	if err := identity.setupCSP(cfg); err != nil {
		return nil, err
	}

	if err := identity.Put(selfPKIID, selfIdentity); err != nil {
		return nil, errors.Wrap(err, "Failed putting out own identity into the identity mapper")
	}
	return identity, nil
}

func (is *identityMapper) Put(pkiID common.PKIidType, identity common.PeerIdentityType) error {
	if pkiID == nil {
		return errors.New("PKIID is nil")
	}
	if identity == nil {
		return errors.New("identity is nil")
	}

	id := is.GetPKIidOfCert(identity)
	if !bytes.Equal(pkiID, id) {
		return errors.New("Identity doesn't match the computed PKIID")
	}

	sid := &protos.SerializedIdentity{}
	err := proto.Unmarshal(identity, sid)
	if err != nil {
		return errors.Wrap(err, "could not unmarshalling a SerializedIdentity")
	}

	cert, err := util.GetX509CertificateFromPEM(sid.IdBytes)
	if err != nil {
		return err
	}

	if _, err := cert.Verify(*is.opts); err != nil {
		return errors.New("could not validate identity against certification chain")
	}

	is.Lock()
	defer is.Unlock()

	if _, exists := is.certs[string(pkiID)]; exists {
		return nil
	}

	var expirationTimer *time.Timer
	expirationDate := cert.NotAfter
	if !expirationDate.IsZero() {
		if time.Now().After(expirationDate) {
			return errors.New("Identity expired")
		}

		timeToLive := expirationDate.Add(time.Millisecond).Sub(time.Now())
		expirationTimer = time.AfterFunc(timeToLive, func() {
			is.delete(pkiID)
		})
	}

	is.certs[string(pkiID)] = newStoredIdentity(pkiID, cert, expirationTimer)
	return nil
}

func (is *identityMapper) Get(pkiID common.PKIidType) (*x509.Certificate, error) {
	is.RLock()
	defer is.RUnlock()

	id, exists := is.certs[string(pkiID)]
	if !exists {
		return nil, errors.New("PKIID wasn't found")
	}
	return id.identity, nil
}

func (is *identityMapper) Sign(msg []byte) ([]byte, error) {
	if is.signer == nil {
		return nil, errors.New("The signer must not be nil")
	}

	digest, err := is.csp.Hash(msg, hash.SHA3256)
	if err != nil {
		return nil, err
	}

	return is.signer.Sign(rand.Reader, digest, nil)
}

func (is *identityMapper) Verify(vkID common.PKIidType, signature, message []byte) error {
	cert, err := is.Get(vkID)
	if err != nil {
		return err
	}

	k, err := is.csp.KeyImport(cert, importer.X509CERT, true)
	if err != nil {
		return err
	}

	digest, err := is.csp.Hash(message, hash.SHA3256)
	if err != nil {
		return err
	}

	valid, err := is.csp.Verify(k, signature, digest, nil)
	if err != nil {
		return errors.Wrap(err, "Could not determine the validity of the signature")
	}
	if !valid {
		return errors.New("The signature is invalid")
	}

	return nil
}

func (is *identityMapper) GetPKIidOfCert(peerIdentity common.PeerIdentityType) common.PKIidType {
	if len(peerIdentity) == 0 {
		logging.Error("Invalid Peer Identity. It must be different from nil")
		return nil
	}

	sid := &protos.SerializedIdentity{}
	err := proto.Unmarshal(peerIdentity, sid)
	if err != nil {
		logging.Errorf("could not unmarshalling a SerializedIdentity: %v", err)
		return nil
	}

	nodeIDRaw := []byte(sid.NodeId)
	raw := append(nodeIDRaw, sid.IdBytes...)

	digest, err := is.csp.Hash(raw, hash.SHA3256)
	if err != nil {
		logging.Errorf("Failed computing digest of serialized identity [% x]: [%s]", peerIdentity, err)
		return nil
	}

	return digest
}

func (is *identityMapper) setupCAs(conf *config.IdentityConfig) error {
	rootCAFiles := conf.GetRootCACerts()
	if len(rootCAFiles) == 0 {
		return errors.New("expected at least one CA certificate")
	}

	is.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	rootCAs := make([]*x509.Certificate, len(rootCAFiles))
	intermediateCAs := make([]*x509.Certificate, len(conf.GetIntermediateCACerts()))
	for i, v := range rootCAFiles {
		certPEM, err := ioutil.ReadFile(v)
		if err != nil {
			return err
		}

		cert, err := util.GetX509CertificateFromPEM(certPEM)
		if err != nil {
			return err
		}
		rootCAs[i] = cert
		is.opts.Roots.AddCert(cert)
	}
	for i, v := range conf.GetIntermediateCACerts() {
		certPEM, err := ioutil.ReadFile(v)
		if err != nil {
			return err
		}

		cert, err := util.GetX509CertificateFromPEM(certPEM)
		if err != nil {
			return err
		}
		intermediateCAs[i] = cert
		is.opts.Intermediates.AddCert(cert)
	}

	is.rootCerts = make([]*x509.Certificate, len(rootCAFiles))
	for i, trustedCert := range rootCAs {
		cert, err := is.sanitizeCert(trustedCert)
		if err != nil {
			return err
		}
		is.rootCerts[i] = cert
	}
	is.intermediateCerts = make([]*x509.Certificate, len(conf.GetIntermediateCACerts()))
	for i, trustedCert := range intermediateCAs {
		cert, err := is.sanitizeCert(trustedCert)
		if err != nil {
			return err
		}
		is.intermediateCerts[i] = cert
	}

	is.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	for _, cert := range is.rootCerts {
		is.opts.Roots.AddCert(cert)
	}
	for _, cert := range is.intermediateCerts {
		is.opts.Intermediates.AddCert(cert)
	}

	return nil
}

func (is *identityMapper) setupCSP(conf *config.IdentityConfig) error {
	certPEM, err := ioutil.ReadFile(conf.GetCertificate())
	if err != nil {
		return err
	}

	cert, err := util.GetX509CertificateFromPEM(certPEM)
	if err != nil {
		return err
	}

	certPubK, err := is.csp.KeyImport(cert, importer.X509CERT, true)
	if err != nil {
		return errors.Wrap(err, "Failed to import certificate's public key")
	}

	id := certPubK.Identifier()
	privateKey, err := is.csp.GetKey(id)
	if err != nil {
		return errors.Wrap(err, "Could not find matching private key for SKI")
	}
	if !privateKey.Private() {
		return errors.Errorf("The private key associated with the certificate with SKI '%s' was not found", hex.EncodeToString(id))
	}

	signer, err := provider.NewSigner(is.csp, privateKey)
	if err != nil {
		return errors.Wrap(err, "Failed to create signer from cccsp")
	}
	is.signer = signer

	return nil
}

func (is *identityMapper) delete(pkiID common.PKIidType) {
	is.Lock()
	defer is.Unlock()
	is.onPurge(pkiID)
	delete(is.certs, string(pkiID))
}

// sanitizeCert ensures that x509 certificates signed using ECDSA
// do have signatures in Low-S. If this is not the case, the certificate
// is regenerated to have a Low-S signature.
func (is *identityMapper) sanitizeCert(cert *x509.Certificate) (*x509.Certificate, error) {
	if isECDSASignedCert(cert) {
		var parentCert *x509.Certificate
		chain, err := is.getUniqueValidationChain(cert)
		if err != nil {
			return nil, err
		}

		if cert.IsCA && len(chain) == 1 {
			parentCert = cert
		} else {
			parentCert = chain[1]
		}

		cert, err = sanitizeECDSASignedCert(cert, parentCert)
		if err != nil {
			return nil, err
		}
	}
	return cert, nil
}

func (is *identityMapper) getUniqueValidationChain(cert *x509.Certificate) ([]*x509.Certificate, error) {
	if is.opts == nil {
		return nil, errors.New("The supplied identity has no verify options")
	}
	validationChains, err := cert.Verify(*is.opts)
	if err != nil {
		return nil, errors.WithMessage(err, "The supplied identity is not valid")
	}

	if len(validationChains) != 1 {
		return nil, errors.Errorf("This Identity only supports a single validation chain, got %d", len(validationChains))
	}

	return validationChains[0], nil
}

type storedIdentity struct {
	pkiID           common.PKIidType
	identity        *x509.Certificate
	expirationTimer *time.Timer
}

func newStoredIdentity(pkiID common.PKIidType, identity *x509.Certificate, expirationTimer *time.Timer) *storedIdentity {
	return &storedIdentity{
		pkiID:           pkiID,
		identity:        identity,
		expirationTimer: expirationTimer,
	}
}
