package cfg

import (
	"fmt"

	"github.com/hashicorp/vault/api"
)

type secreter interface {
	GetSecret(path, key string) (string, error)
}

type vault struct {
	c *api.Client
}

func newVault(addr, token string) (*vault, error) {
	conf := api.DefaultConfig()
	conf.Address = addr
	c, err := api.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("can't create client: %w", err)
	}

	c.SetToken(token)

	return &vault{c: c}, nil
}

func (v *vault) GetSecret(path, key string) (string, error) {
	c := v.c
	secret, err := c.Logical().Read(path)
	if err != nil {
		return "", fmt.Errorf("can't get secret: %w", err)
	}

	str, ok := secret.Data[key].(string)
	if !ok {
		return "", fmt.Errorf("can't get 'key' of the secret: %q", key)
	}

	return str, nil
}
