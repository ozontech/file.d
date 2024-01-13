package file

import (
	"runtime"
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProvierWatcherPaths(t *testing.T) {
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
				Include: []string{"**/*"},
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
				Include: []string{"**/error.log"},
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
				Include: []string{"nginx-ingress-*/error.log"},
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
				Include: []string{"nginx-ingress-*/*"},
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
					Include: []string{"access.log"},
				},
			},
			expectedPathes: Paths{
				Include: []string{"access.log"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctl := metric.New("test", prometheus.NewRegistry())
			testMetric := ctl.RegisterCounter("worker", "help_test")
			config := tt.config
			test.NewConfig(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
			jp := NewJobProvider(config, testMetric, testMetric, &zap.SugaredLogger{})

			assert.Equal(t, tt.expectedPathes, jp.watcher.paths)
		})
	}
}
