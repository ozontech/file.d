package file

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rjeczalik/notify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestWatcher(t *testing.T) {
	tests := []struct {
		name            string
		filenamePattern string
	}{
		{
			name:            "should_ok_and_count_only_creation",
			filenamePattern: "watch*.log",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			shouldCreate := atomic.Int64{}
			notifyFn := func(_ notify.Event, _ string, _ os.FileInfo) {
				shouldCreate.Inc()
			}
			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			w := NewWatcher(
				dir,
				Paths{
					Include: []string{
						filepath.Join(
							dir,
							tt.filenamePattern,
						),
					},
				},
				notifyFn,
				false,
				ctl.RegisterGauge("worker", "help_test"),
				zap.L().Sugar(),
			)
			w.start()
			defer w.stop()

			// create, write, remove files and ensure events are only passed for creation events.

			f1Name := filepath.Join(dir, "watch1.log")

			f1, err := os.Create(f1Name)
			require.NoError(t, err)
			err = f1.Close()
			require.NoError(t, err)

			f2, err := os.Create(filepath.Join(dir, "watch2.log"))
			require.NoError(t, err)
			err = f2.Close()
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			f1, err = os.OpenFile(f1Name, os.O_WRONLY, 0o600)
			require.NoError(t, err)
			_, err = fmt.Fprint(f1, "test")
			require.NoError(t, err)
			err = f1.Close()
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			err = os.Remove(f1Name)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			require.Equal(t, int64(2), shouldCreate.Load())
		})
	}
}

// nolint:gocritic
func TestWatcherPaths(t *testing.T) {
	dir := t.TempDir()
	shouldCreate := atomic.Int64{}
	notifyFn := func(_ notify.Event, _ string, _ os.FileInfo) {
		shouldCreate.Inc()
	}
	ctl := metric.NewCtl("test", prometheus.NewRegistry())
	w := NewWatcher(
		dir,
		Paths{
			Include: []string{
				filepath.Join(dir, "nginx-ingress-*/error.log"),
				filepath.Join(dir, "log/**/*"),
				filepath.Join(dir, "access.log"),
				filepath.Join(dir, "**/sub_access.log"),
			},
			Exclude: []string{
				filepath.Join(dir, "log/payments/**"),
				filepath.Join(dir, "nginx-ingress-payments/error.log"),
			},
		},
		notifyFn,
		false,
		ctl.RegisterGauge("worker", "help_test"),
		zap.L().Sugar(),
	)
	w.start()
	defer w.stop()

	tests := []struct {
		name         string
		filename     string
		shouldNotify bool
	}{
		{
			filename:     filepath.Join(dir, "nginx-ingress-0/error.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "log/errors.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "log/0/errors.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "log/0/0/errors.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "access.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "sub_access.log"),
			shouldNotify: true,
		},
		{
			filename:     filepath.Join(dir, "access1.log"),
			shouldNotify: false,
		},
		{
			filename:     filepath.Join(dir, "nginx/errors.log"),
			shouldNotify: false,
		},
		{
			filename:     filepath.Join(dir, "log/payments/errors.log"),
			shouldNotify: false,
		},
		{
			filename:     filepath.Join(dir, "log/payments/nginx-ingress-0/errors.log"),
			shouldNotify: false,
		},
		{
			filename:     filepath.Join(dir, "nginx-ingress-payments/error.log"),
			shouldNotify: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		relFilename, _ := filepath.Rel(dir, tt.filename)
		t.Run(relFilename, func(t *testing.T) {
			filename := tt.filename

			err := os.MkdirAll(filepath.Dir(filename), 0o700)
			require.NoError(t, err)

			f1, err := os.Create(filename)
			require.NoError(t, err)
			err = f1.Close()
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)
			before := shouldCreate.Load()
			w.notify(notify.Create, filename)
			after := shouldCreate.Load()

			isNotified := after-before != 0
			require.Equal(t, tt.shouldNotify, isNotified)
		})
	}
}

func TestCommonPathPrefix(t *testing.T) {
	a := assert.New(t)

	paths := []string{
		"/var/log/",
		"/var/lib/docker/",
	}

	result := commonPathPrefix(paths)
	a.Equal("/var", result)
}
