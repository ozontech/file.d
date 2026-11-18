// Package journald is the renamed journalctl input plugin.
//
// journalctl is a tool; journald is the daemon. This plugin reads from
// journald, so the canonical name is now 'journald'. The old 'journalctl'
// name is kept as a deprecated alias (resolves issue #200).
//
// To generate documentation: make gen-doc
package journald

import (
	// Re-export everything from the original journalctl package so that
	// the journald package is a transparent alias. Both type names are
	// registered in init() below.
	"github.com/ozontech/file.d/plugin/input/journalctl"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

func init() {
	// Primary name: journald
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "journald",
		Factory: journalctl.Factory,
	})
}
