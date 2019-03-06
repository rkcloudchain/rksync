package util

import (
	"crypto/ecdsa"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"reflect"

	"github.com/pkg/errors"
)

// Equals returns whether a and b are the same
type Equals func(a interface{}, b interface{}) bool

// ComputeSHA256 returns SHA2-256 on data
func ComputeSHA256(data []byte) (hash []byte) {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

// PEMToX509Certs parse PEM-encoded certs
func PEMToX509Certs(pemCert []byte) ([]*x509.Certificate, []string, error) {
	certs := []*x509.Certificate{}
	subjects := []string{}
	for len(pemCert) > 0 {
		var block *pem.Block
		block, pemCert = pem.Decode(pemCert)
		if block == nil {
			break
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, subjects, err
		}

		certs = append(certs, cert)
		subjects = append(subjects, string(cert.RawSubject))
	}

	return certs, subjects, nil
}

// GetX509CertificateFromPEM get on x509 certificate from bytes in PEM format
func GetX509CertificateFromPEM(cert []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(cert)
	if block == nil {
		return nil, errors.New("Failed to PEM decode certificate")
	}
	x509Cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing certificate")
	}
	return x509Cert, nil
}

// GetRSAPrivateKey get *rsa.PrivateKey from key pem
func GetRSAPrivateKey(raw []byte) (*rsa.PrivateKey, error) {
	decoded, _ := pem.Decode(raw)
	if decoded == nil {
		return nil, errors.New("Failed to decode the PEM-encoded RSA key")
	}
	RSAPrivKey, err := x509.ParsePKCS1PrivateKey(decoded.Bytes)
	if err == nil {
		return RSAPrivKey, nil
	}
	key, err2 := x509.ParsePKCS8PrivateKey(decoded.Bytes)
	if err2 == nil {
		switch key.(type) {
		case *ecdsa.PrivateKey:
			return nil, errors.New("Expecting RSA private key but found EC private key")
		case *rsa.PrivateKey:
			return key.(*rsa.PrivateKey), nil
		default:
			return nil, errors.New("Invalid private key type in PKCS#8 wrapping")
		}
	}
	return nil, errors.Wrap(err, "Failed parsing RSA private key")
}

// GetECPrivateKey get *ecdsa.PrivateKey from key pem
func GetECPrivateKey(raw []byte) (*ecdsa.PrivateKey, error) {
	decoded, _ := pem.Decode(raw)
	if decoded == nil {
		return nil, errors.New("Failed to decode the PEM-encoded ECDSA key")
	}
	ECPrivKey, err := x509.ParseECPrivateKey(decoded.Bytes)
	if err == nil {
		return ECPrivKey, nil
	}
	key, err2 := x509.ParsePKCS8PrivateKey(decoded.Bytes)
	if err2 == nil {
		switch key.(type) {
		case *ecdsa.PrivateKey:
			return key.(*ecdsa.PrivateKey), nil
		case *rsa.PrivateKey:
			return nil, errors.New("Expecting EC private key but found RSA private key")
		default:
			return nil, errors.New("Invalid private key type in PKCS#8 wrapping")
		}
	}
	return nil, errors.Wrap(err, "Failed parsing EC private key")
}

// RandomUInt64 returns a random uint64
func RandomUInt64() uint64 {
	b := make([]byte, 8)
	_, err := io.ReadFull(cryptorand.Reader, b)
	if err == nil {
		n := new(big.Int)
		return n.SetBytes(b).Uint64()
	}
	rand.Seed(rand.Int63())
	return uint64(rand.Int63())
}

func numbericEqual(a interface{}, b interface{}) bool {
	return a.(int) == b.(int)
}

// GetRandomIndices returns a slice of random indices
// from 0 to given highestIndex
func GetRandomIndices(indiceCount, highestIndex int) []int {
	if highestIndex+1 < indiceCount {
		return nil
	}

	indices := make([]int, 0)
	if highestIndex+1 == indiceCount {
		for i := 0; i < indiceCount; i++ {
			indices = append(indices, i)
		}
		return indices
	}

	for len(indices) < indiceCount {
		n := RandomInt(highestIndex + 1)
		if IndexInSlice(indices, n, numbericEqual) != -1 {
			continue
		}
		indices = append(indices, n)
	}
	return indices
}

// IndexInSlice returns the index of given object o in array
func IndexInSlice(array interface{}, o interface{}, equals Equals) int {
	arr := reflect.ValueOf(array)
	for i := 0; i < arr.Len(); i++ {
		if equals(arr.Index(i).Interface(), o) {
			return i
		}
	}
	return -1
}

// RandomInt returns, as an int, a non-negative pseudo-random integer in [0,n)
// It panics if n <= 0
func RandomInt(n int) int {
	if n <= 0 {
		panic(fmt.Sprintf("Got invalid (non positive) value: %d", n))
	}
	m := int(RandomUInt64()) % n
	if m < 0 {
		return n + m
	}
	return m
}
