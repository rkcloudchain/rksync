package identity

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
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

type identityMapper struct {
	certs     map[string]*x509.Certificate
	opts      *x509.VerifyOptions
	rootCerts []*x509.Certificate
	privKey   interface{}
	sync.RWMutex
}

// NewIdentity returns a new Identity instance
func NewIdentity(cfg *config.IdentityConfig, selfIdentity common.PeerIdentityType) (Identity, error) {
	if cfg == nil {
		return nil, errors.New("NewIdentity error: nil cfg reference")
	}

	logging.Debug("Creating Identity instance")
	identity := &identityMapper{
		certs: make(map[string]*x509.Certificate),
	}

	selfPKIID := identity.GetPKIidOfCert(selfIdentity)

	if err := identity.setupCAs(cfg); err != nil {
		return nil, err
	}

	if err := identity.setupCSP(cfg); err != nil {
		return nil, err
	}

	if err := identity.Put(selfPKIID, selfIdentity); err != nil {
		panic(errors.Wrap(err, "Failed putting out own identity into the identity mapper"))
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

	if _, exists := is.certs[hex.EncodeToString(pkiID)]; exists {
		return nil
	}

	is.certs[hex.EncodeToString(pkiID)] = cert
	return nil
}

func (is *identityMapper) Get(pkiID common.PKIidType) (*x509.Certificate, error) {
	is.RLock()
	defer is.RUnlock()

	cert, exists := is.certs[hex.EncodeToString(pkiID)]
	if !exists {
		return nil, errors.New("PKIID wasn't found")
	}
	return cert, nil
}

func (is *identityMapper) Sign(msg []byte) ([]byte, error) {
	if is.privKey == nil {
		return nil, errors.New("expected the private key")
	}

	privateKey := is.privKey
	switch privateKey.(type) {
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(rand.Reader, privateKey.(*ecdsa.PrivateKey), msg)
		if err != nil {
			return nil, err
		}

		s, _, err = util.ToLowS(&privateKey.(*ecdsa.PrivateKey).PublicKey, s)
		if err != nil {
			return nil, err
		}

		return util.MarshalECDSASignature(r, s)

	case *rsa.PrivateKey:
		return privateKey.(*rsa.PrivateKey).Sign(rand.Reader, msg, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto, Hash: crypto.SHA256})

	default:
		return nil, errors.New("Unsupported secret key type")
	}
}

func (is *identityMapper) Verify(vkID common.PKIidType, signature, message []byte) error {
	cert, err := is.Get(vkID)
	if err != nil {
		return err
	}

	switch cert.PublicKey.(type) {
	case *ecdsa.PublicKey:
		publicKey := cert.PublicKey.(*ecdsa.PublicKey)
		r, s, err := util.UnmarshalECDSASignature(signature)
		if err != nil {
			return fmt.Errorf("Failed unmarshalling signature [%s]", err)
		}

		lowS, err := util.IsLowS(publicKey, s)
		if err != nil {
			return err
		}

		if !lowS {
			return fmt.Errorf("Invalid S. Must be smaller than half the order [%s][%s]", s, util.GetCurveHalfOrdersAt(publicKey.Curve))
		}

		valid := ecdsa.Verify(publicKey, message, r, s)
		if !valid {
			return errors.New("The signature is invalid")
		}
		return nil

	case *rsa.PublicKey:
		err := rsa.VerifyPSS(cert.PublicKey.(*rsa.PublicKey), crypto.SHA256, message, signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto})
		if err != nil {
			return errors.WithMessage(err, "could not determine the validity of the signature")
		}
		return nil

	default:
		return errors.New("Unsupported secret key type")
	}
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

	return util.ComputeSHA256(raw)
}

func (is *identityMapper) setupCAs(conf *config.IdentityConfig) error {
	if len(conf.CAs) == 0 {
		return errors.New("expected at least one CA certificate")
	}

	is.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	rootCAs := make([]*x509.Certificate, len(conf.CAs))
	for i, v := range conf.CAs {
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

	is.rootCerts = make([]*x509.Certificate, len(conf.CAs))
	for i, trustedCert := range rootCAs {
		cert, err := is.sanitizeCert(trustedCert)
		if err != nil {
			return err
		}

		is.rootCerts[i] = cert
	}

	is.opts = &x509.VerifyOptions{Roots: x509.NewCertPool(), Intermediates: x509.NewCertPool()}
	for _, cert := range is.rootCerts {
		is.opts.Roots.AddCert(cert)
	}

	return nil
}

func (is *identityMapper) setupCSP(conf *config.IdentityConfig) error {
	if conf.Certificate == "" {
		return errors.New("expected a certificate file")
	}
	if conf.Key == "" {
		return errors.New("expected a key file")
	}

	certPEM, err := ioutil.ReadFile(conf.Certificate)
	if err != nil {
		return err
	}
	keyPEM, err := ioutil.ReadFile(conf.Key)
	if err != nil {
		return err
	}

	cert, err := util.GetX509CertificateFromPEM(certPEM)
	if err != nil {
		return err
	}

	pubKey := cert.PublicKey
	switch pubKey.(type) {
	case *rsa.PublicKey:
		privKey, err := util.GetRSAPrivateKey(keyPEM)
		if err != nil {
			return err
		}
		if privKey.PublicKey.N.Cmp(pubKey.(*rsa.PublicKey).N) != 0 {
			return errors.New("Public key and private key do not match")
		}
		is.privKey = privKey

	case *ecdsa.PublicKey:
		privKey, err := util.GetECPrivateKey(keyPEM)
		if err != nil {
			return err
		}
		if privKey.PublicKey.X.Cmp(pubKey.(*ecdsa.PublicKey).X) != 0 {
			return errors.New("Public key and private key do not match")
		}
		is.privKey = privKey

	default:
		return errors.New("Unsupported secret key type")
	}

	return nil
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
