package xoauth

import (
	"errors"
)

type AuthStyle int

const (
	AuthStyleUnknown AuthStyle = iota
	AuthStyleInParams
	AuthStyleInHeader
)

type TLSConfig struct {
	CACert     string
	ClientCert string
	ClientKey  string
	Insecure   bool
}

type Config struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
	AuthStyle    AuthStyle

	TLS *TLSConfig
}

func (c *Config) validate() error {
	if c.ClientID == "" {
		return errors.New("client id must be non-empty")
	}
	if c.TokenURL == "" {
		return errors.New("token url must be non-empty")
	}
	if c.AuthStyle == AuthStyleUnknown {
		return errors.New("auth style must be specified")
	}
	if tls := c.TLS; tls != nil {
		if tls.ClientCert != "" && tls.ClientKey == "" ||
			tls.ClientCert == "" && tls.ClientKey != "" {
			return errors.New("both client cert and client key must be provided")
		}
	}
	return nil
}
