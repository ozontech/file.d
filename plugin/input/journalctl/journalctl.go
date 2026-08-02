package journalctl

import (
	"os/exec"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

// Config holds the configuration for the journalctl/journald input plugin.
type Config struct {
	// Offset is the cursor or position in the journal to start reading from.
	Offset string `json:"offset" default:"" description:"Journal cursor to start from. Empty means start from the end."`

	// MaxLines is the maximum number of lines to read per iteration.
	MaxLines int `json:"max_lines" default:"1000" description:"Maximum lines to read per poll cycle."`
}

// Plugin reads events from the systemd journal using journalctl.
type Plugin struct {
	config *Config
	logger *zap.Logger
	cmd    *exec.Cmd
}

// Factory returns a new Plugin instance.
func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func init() {
	// Register under the legacy 'journalctl' name for backward compatibility.
	// New configurations should use 'journald' (see plugin/input/journald).
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "journalctl",
		Factory: Factory,
	})
}
