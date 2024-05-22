package xoauth

import "errors"

type Config struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	TokenURL     string `json:"token_url"`
}

func (c Config) validate() error {
	if c.ClientID == "" {
		return errors.New("'client_id' must be set")
	}
	if c.ClientSecret == "" {
		return errors.New("'client_secret' must be set")
	}
	if c.TokenURL == "" {
		return errors.New("'token_url' must be set")
	}
	return nil
}
