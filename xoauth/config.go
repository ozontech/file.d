package xoauth

import "fmt"

type AuthStyle int

const (
	AuthStyleUnknown AuthStyle = iota
	AuthStyleInParams
	AuthStyleInHeader
)

type Config struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
	AuthStyle    AuthStyle
}

func (c *Config) validate() error {
	if c.ClientID == "" {
		return fmt.Errorf("client id must be non-empty")
	}
	if c.TokenURL == "" {
		return fmt.Errorf("token url must be non-empty")
	}
	if c.AuthStyle == AuthStyleUnknown {
		return fmt.Errorf("auth style must be specified")
	}
	return nil
}
