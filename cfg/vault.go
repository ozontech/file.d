package cfg

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/hashicorp/vault/api"
	auth "github.com/hashicorp/vault/api/auth/approle"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/xtls"
)

type secreter interface {
	GetSecret(path, key string) (string, error)
}

type vault struct {
	c *api.Client
}

func newVault(cfg *VaultConfig) (*vault, error) {
	if cfg == nil {
		return nil, nil
	}

	conf := api.DefaultConfig()
	conf.Address = cfg.Address
	if tls := cfg.TLS; tls != nil {
		b := xtls.NewConfigBuilder()
		if tls.CACert != "" {
			if err := b.AppendCARoot(tls.CACert); err != nil {
				return nil, fmt.Errorf("can't append CA root: %w", err)
			}
		}
		if tls.ClientCert != "" && tls.ClientKey != "" {
			if err := b.AppendX509KeyPair(tls.ClientCert, tls.ClientKey); err != nil {
				return nil, fmt.Errorf("can't append X509 key pair: %w", err)
			}
		}
		b.SetSkipVerify(tls.Insecure)

		transport, _ := conf.HttpClient.Transport.(*http.Transport)
		transport.TLSClientConfig = b.Build()
	}

	c, err := api.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("can't create api client: %w", err)
	}

	if cfg.Token != "" {
		c.SetToken(cfg.Token)
	} else {
		appRoleAuth, err := auth.NewAppRoleAuth(
			cfg.RoleID,
			&auth.SecretID{FromString: cfg.SecretID},
			auth.WithMountPath(cfg.AuthMountPath),
		)
		if err != nil {
			return nil, fmt.Errorf("can't create approle auth: %w", err)
		}

		// after a successful login, this method will automatically set the client’s token
		_, err = c.Auth().Login(context.Background(), appRoleAuth)
		if err != nil {
			return nil, err
		}
	}

	return &vault{c: c}, nil
}

func (v *vault) GetSecret(path, key string) (string, error) {
	if v == nil || v.c == nil {
		logger.Fatalf("can't get secret without vault api client")
	}

	secret, err := v.c.Logical().Read(path)
	if err != nil {
		return "", fmt.Errorf("can't get secret %q: %w", path, err)
	}

	str, ok := secret.Data[key].(string)
	if !ok {
		return "", fmt.Errorf("can't get key %q of the secret %q", key, path)
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
