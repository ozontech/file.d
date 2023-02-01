package cfg

import (
	"os"
	"strings"

	"github.com/ozontech/file.d/logger"
)

type envs struct{}

func (e *envs) tryApply(s string) (string, bool) {
	// escape symbols.
	if strings.HasPrefix(s, `\env(`) {
		s = strings.ReplaceAll(s, `\env(`, "env(")
		return s, true
	}

	if !strings.HasPrefix(s, "env(") || !strings.HasSuffix(s, ")") {
		return "", false
	}

	envName := strings.TrimPrefix(s, "env(")
	envName = strings.TrimSuffix(envName, ")")

	env, ok := os.LookupEnv(envName)
	if !ok {
		logger.Fatalf("can't GetEnv: %s", envName)
	}
	return env, true
}
