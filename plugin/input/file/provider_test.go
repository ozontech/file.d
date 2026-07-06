package file

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"runtime"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/xtime"
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
	ctl := metric.NewCtl("test", prometheus.NewRegistry(), time.Minute, 0)
	metrics := newMetricCollection(
		ctl.RegisterCounter("worker1", "help_test"),
		ctl.RegisterCounter("worker2", "help_test"),
		ctl.RegisterGauge("worker3", "help_test"),
		ctl.RegisterGauge("worker4", "help_test"),
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
			ctl := metric.NewCtl("test", prometheus.NewRegistry(), time.Minute, 0)
			config := tt.config
			test.NewConfig(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})
			metrics := newMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
				ctl.RegisterCounter("worker2", "help_test"),
				ctl.RegisterGauge("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
			)
			jp := NewJobProvider(config, metrics, &zap.SugaredLogger{})

			assert.Equal(t, tt.expectedPathes, jp.watcher.paths)
		})
	}
}

func TestEOFInfo(t *testing.T) {
	t.Run("setTimestamp and getTimestamp match", func(t *testing.T) {
		e := &eofInfo{}

		now := time.Now()
		e.setTimestamp(now)

		got := e.getTimestamp()
		assert.Equal(t, now.UnixNano(), got.UnixNano())
		assert.True(t, now.Equal(got))
	})

	t.Run("setUnixTimestamp and getUnixTimestamp match", func(t *testing.T) {
		e := &eofInfo{}

		expected := time.Now().Unix()
		e.setUnixNanoTimestamp(expected)

		got := e.getUnixNanoTimestamp()
		assert.Equal(t, expected, got)
	})

	t.Run("setOffset and getOffset match", func(t *testing.T) {
		e := &eofInfo{}

		expected := int64(12345)
		e.setOffset(expected)

		got := e.getOffset()
		assert.Equal(t, expected, got)
	})

	t.Run("concurrent access to timestamp", func(t *testing.T) {
		e := &eofInfo{}
		var wg sync.WaitGroup

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ts := time.Unix(int64(idx), 0)
				e.setTimestamp(ts)
				_ = e.getTimestamp() // Just read it
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent access to offset", func(t *testing.T) {
		e := &eofInfo{}
		var wg sync.WaitGroup

		// Start multiple goroutines writing different offsets
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(offset int64) {
				defer wg.Done()
				e.setOffset(offset)
				_ = e.getOffset() // Just read it
			}(int64(i * 100))
		}

		wg.Wait()
	})

	t.Run("timestamp and unix timestamp consistency", func(t *testing.T) {
		e := &eofInfo{}

		now := time.Now()
		e.setTimestamp(now)

		// Verify unix timestamp matches
		unixTs := e.getUnixNanoTimestamp()
		assert.Equal(t, now.UnixNano(), unixTs)

		// Now set via unix timestamp and verify via getTimestamp
		newUnixTs := now.Add(time.Hour).Unix()
		e.setUnixNanoTimestamp(newUnixTs)

		gotTime := e.getTimestamp()
		assert.Equal(t, newUnixTs, gotTime.UnixNano())
	})

	t.Run("timestamp and unix timestamp consistency with xtime", func(t *testing.T) {
		e := &eofInfo{}

		now := xtime.GetInaccurateTime()
		e.setTimestamp(now)

		// Verify unix timestamp matches
		unixTs := e.getUnixNanoTimestamp()
		assert.Equal(t, now.UnixNano(), unixTs)

		// Now set via unix timestamp and verify via getTimestamp
		newUnixTs := xtime.GetInaccurateUnixNano()
		e.setUnixNanoTimestamp(newUnixTs)

		gotTime := e.getTimestamp()
		assert.Equal(t, newUnixTs, gotTime.UnixNano())
	})
}
