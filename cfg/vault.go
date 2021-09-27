package cfg

import (
	"github.com/hashicorp/vault/api"
	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "can't create client")
	}

	c.SetToken(token)

	return &vault{c: c}, nil
}

func (v *vault) GetSecret(path, key string) (string, error) {
	c := v.c
	secret, err := c.Logical().Read(path)
	if err != nil {
		return "", errors.Wrap(err, "can't get secret")
	}

	str, ok := secret.Data[key].(string)
	if !ok {
		return "", errors.Wrap(err, "can't get 'value' of the secret")
	}

	return str, nil
}
