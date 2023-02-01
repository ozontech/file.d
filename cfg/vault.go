package cfg

import (
	"fmt"
	"strings"

	"github.com/hashicorp/vault/api"
	"github.com/ozontech/file.d/logger"
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
	if v.c == nil {
		logger.Fatalf("can't get secret without connection vault api.Client")
	}
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

func (v *vault) tryApply(s string) (string, bool) {
	return tryApplySecreter(v, s)
}

func tryApplySecreter(secreter secreter, s string) (string, bool) {
	// escape symbols.
	if strings.HasPrefix(s, `\vault(`) {
		s = strings.ReplaceAll(s, `\vault(`, "vault(")
		return s, true
	}

	if !strings.HasPrefix(s, "vault(") || !strings.HasSuffix(s, ")") {
		return "", false
	}

	args := strings.TrimPrefix(s, "vault(")
	args = strings.TrimSuffix(args, ")")
	noSpaces := strings.ReplaceAll(args, " ", "")
	pathAndKey := strings.Split(noSpaces, ",")

	logger.Infof("get secrets for %q and %q", pathAndKey[0], pathAndKey[1])
	secret, err := secreter.GetSecret(pathAndKey[0], pathAndKey[1])
	if err != nil {
		logger.Fatalf("can't GetSecret: %s", err.Error())
	}

	logger.Infof("success getting secret %q and %q", pathAndKey[0], pathAndKey[1])
	return secret, true
}
