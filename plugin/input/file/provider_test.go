package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
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
