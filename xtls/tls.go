package xtls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

var (
	ErrEmptyCert = errors.New("both CA cert and cert key must be set")

	pemBegin = []byte("-----BEGIN")
)

type ConfigBuilder struct {
	cfg      *tls.Config
	readFile func(name string) ([]byte, error)
}

// NewConfigBuilder returns new TLSConfigBuilder builder.
// It helps to set up the tls config.
func NewConfigBuilder() ConfigBuilder {
	return ConfigBuilder{
		cfg:      &tls.Config{},
		readFile: os.ReadFile,
	}
}

// AppendX509KeyPair appends CA cert and key to the tls config.
// if either is a path to an encoded PEM file,
// the contents of the file are read first and then the contents are added to the tls config.
func (b ConfigBuilder) AppendX509KeyPair(caCert, certKey string) error {
	if caCert == "" || certKey == "" {
		return ErrEmptyCert
	}

	var (
		caCertContent = []byte(caCert)
		err           error
	)
	// is this path to the PEM encoded CA cert?
	if !isPEM(caCertContent) {
		caCertContent, err = b.readFile(caCert)
		if err != nil {
			return fmt.Errorf("can't read CA cert file=%q: %w", caCert, err)
		}
	}

	certKeyContent := []byte(certKey)
	// is this path to the PEM encoded private key?
	if !isPEM(certKeyContent) {
		certKeyContent, err = b.readFile(certKey)
		if err != nil {
			return fmt.Errorf("can't read private key file=%q: %w", certKey, err)
		}
	}

	tlsCert, err := tls.X509KeyPair(caCertContent, certKeyContent)
	if err != nil {
		return fmt.Errorf("can't load x509 key pair: %w", err)
	}

	b.cfg.Certificates = append(b.cfg.Certificates, tlsCert)

	return nil
}

// AppendCARoot appends certificates to the tls config.
// If caCert is a path to a PEM encoded file, it reads and appends the content to the tls config.
// If RootCAs is nil, TLS uses the host's root CA set.
func (b ConfigBuilder) AppendCARoot(caCert string) error {
	if caCert == "" {
		return ErrEmptyCert
	}

	var (
		certContent = []byte(caCert)
		err         error
	)
	// is this path to the PEM encoded CA cert?
	if !isPEM(certContent) {
		certContent, err = b.readFile(caCert)
		if err != nil {
			return fmt.Errorf("can't read CA cert file=%q: %w", caCert, err)
		}
	}

	b.cfg.RootCAs = x509.NewCertPool()

	// certContent can contain many certificates, we have to parse them all
	for len(certContent) > 0 {
		var block *pem.Block
		block, certContent = pem.Decode(certContent)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("can't parse CA cert: %w", err)
		}

		b.cfg.RootCAs.AddCert(cert)
	}

	return nil
}

func (b ConfigBuilder) SetSkipVerify(val bool) {
	b.cfg.InsecureSkipVerify = val
}

// Build returns built tls config.
func (b ConfigBuilder) Build() *tls.Config {
	return b.cfg
}

// isPEM checks if the content is PEM encoded.
func isPEM(content []byte) bool {
	return bytes.Contains(content, pemBegin)
}
