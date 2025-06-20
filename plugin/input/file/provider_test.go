package file

import (
	"os"
	"path/filepath"
	"testing"

	"runtime"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRefreshSymlinkOnBrokenLink(t *testing.T) {
	file, err := os.CreateTemp("", "input_file")
	if err != nil {
		panic(err.Error())
	}

	linkName := filepath.Join(os.TempDir(), uuid.NewV4().String())
	err = os.Symlink(file.Name(), linkName)
	if err != nil {
		panic(err.Error())
	}
	defer os.Remove(linkName)
	ctl := metric.NewCtl("test", prometheus.NewRegistry())
	metrics := newMetricCollection(
		ctl.RegisterCounter("worker1", "help_test"),
		ctl.RegisterCounter("worker2", "help_test"),
		ctl.RegisterCounter("worker3", "help_test"),
		ctl.RegisterGauge("worker4", "help_test"),
		ctl.RegisterGauge("worker5", "help_test"),
	)
	jp := NewJobProvider(&Config{
		MaxFiles: 100,
	}, metrics, logger.Instance)
	jp.addSymlink(1, linkName)
	jp.maintenanceSymlinks()
	require.Equal(t, 1, len(jp.symlinks))

	// broke link
	os.Remove(file.Name())
	jp.maintenanceSymlinks()
	require.Equal(t, 1, len(jp.symlinks))

	// delete link
	os.Remove(linkName)
	jp.maintenanceSymlinks()
	require.Equal(t, 0, len(jp.symlinks))
}

// nolint:gocritic
func TestProviderWatcherPaths(t *testing.T) {
	currentDir, _ := os.Getwd()
	tests := []struct {
		name           string
		config         *Config
		expectedPathes Paths
	}{
		{
			name: "default config",
			config: &Config{
				WatchingDir: "/var/log/",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{"/var/log/**/*"},
			},
		},
		{
			name: "filename pattern config",
			config: &Config{
				WatchingDir:     "/var/log/",
				FilenamePattern: "error.log",
				OffsetsFile:     "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{"/var/log/**/error.log"},
			},
		},
		{
			name: "filename and dir pattern config",
			config: &Config{
				WatchingDir:     "/var/log/",
				FilenamePattern: "error.log",
				DirPattern:      "nginx-ingress-*",
				OffsetsFile:     "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{"/var/log/nginx-ingress-*/error.log"},
			},
		},
		{
			name: "dir pattern config",
			config: &Config{
				WatchingDir: "/var/log/",
				DirPattern:  "nginx-ingress-*",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{"/var/log/nginx-ingress-*/*"},
			},
		},
		{
			name: "ignore filename and dir pattern",
			config: &Config{
				WatchingDir:     "/var/log/",
				FilenamePattern: "error.log",
				DirPattern:      "nginx-ingress-*",
				OffsetsFile:     "offset.json",
				Paths: Paths{
					Include: []string{"/var/log/access.log"},
				},
			},
			expectedPathes: Paths{
				Include: []string{"/var/log/access.log"},
			},
		},
		{
			name: "relative paths",
			config: &Config{
				WatchingDir: "./var/log/",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{filepath.Join(currentDir, "var/log/**/*")},
			},
		},
		{
			name: "relative paths 2",
			config: &Config{
				WatchingDir: "var/log/",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{filepath.Join(currentDir, "var/log/**/*")},
			},
		},
		{
			name: "wildcard dir",
			config: &Config{
				WatchingDir: "*",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{filepath.Join(currentDir, "**/*")},
			},
		},
		{
			name: "dir pattern 2",
			config: &Config{
				WatchingDir: "*",
				DirPattern:  "host*",
				OffsetsFile: "offset.json",
			},
			expectedPathes: Paths{
				Include: []string{filepath.Join(currentDir, "/host*/*")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			config := tt.config
			test.NewConfig(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
			metrics := newMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
				ctl.RegisterCounter("worker2", "help_test"),
				ctl.RegisterCounter("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
				ctl.RegisterGauge("worker5", "help_test"),
			)
			jp := NewJobProvider(config, metrics, &zap.SugaredLogger{})

			assert.Equal(t, tt.expectedPathes, jp.watcher.paths)
		})
	}
}
