//go:build linux

package file

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rjeczalik/notify"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWatcherFlatPatternsExcludeSnapshotTree(t *testing.T) {
	root := t.TempDir()
	logs := filepath.Join(root, "logs")
	containerd := filepath.Join(root, "containerd")
	snapshots := filepath.Join(containerd, "snapshots")
	for _, dir := range []string{logs, snapshots} {
		require.NoError(t, os.MkdirAll(dir, 0755))
	}
	events := make(chan string, 32)
	ctl := metric.NewCtl("scope", prometheus.NewRegistry(), time.Minute, 0)
	w := NewWatcher(root, Paths{Include: []string{filepath.Join(logs, "kubelet.log*"), filepath.Join(containerd, "containerd.log*")}},
		func(_ notify.Event, path string, _ os.FileInfo) { events <- path }, true, ctl.RegisterGauge("queue", ""), zap.NewNop().Sugar())
	w.start()
	defer w.stop()

	watches := inotifyInodes(t)
	for _, dir := range []string{logs, containerd} {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.True(t, watches[info.Sys().(*syscall.Stat_t).Ino], "missing log directory watch: %s", dir)
	}
	info, err := os.Stat(snapshots)
	require.NoError(t, err)
	require.False(t, watches[info.Sys().(*syscall.Stat_t).Ino], "flat log masks must not install recursive snapshot watches")

	for _, path := range []string{filepath.Join(logs, "kubelet.log"), filepath.Join(containerd, "containerd.log")} {
		require.NoError(t, os.WriteFile(path, []byte("before rotation\n"), 0600))
		waitWatcherPath(t, events, path)
		require.NoError(t, os.Rename(path, path+".1"))
		require.NoError(t, os.WriteFile(path, []byte("after rotation\n"), 0600))
		waitWatcherPath(t, events, path+".1", path)
	}
}

func TestWatcherRecursivePatternDiscoversNewDirectories(t *testing.T) {
	testWatcherNewDirectory(t, "**/app.log")
}

func TestWatcherFlatPatternsDiscoverMissingDirectories(t *testing.T) {
	testWatcherNewDirectory(t, "new/deep/app.log")
}

func testWatcherNewDirectory(t *testing.T, pattern string) {
	t.Helper()
	root := t.TempDir()
	events := make(chan string, 32)
	ctl := metric.NewCtl("scope", prometheus.NewRegistry(), time.Minute, 0)
	// the second path keeps the common ancestor present when the first parent is missing
	w := NewWatcher(root, Paths{Include: []string{filepath.Join(root, pattern), filepath.Join(root, "other.log")}},
		func(_ notify.Event, path string, _ os.FileInfo) { events <- path }, false, ctl.RegisterGauge("queue", ""), zap.NewNop().Sugar())
	w.start()
	defer w.stop()
	path := filepath.Join(root, "new/deep/app.log")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, []byte("marker\n"), 0600))
	waitWatcherPath(t, events, path)
}

func waitWatcherPath(t *testing.T, events <-chan string, paths ...string) {
	t.Helper()
	want := make(map[string]bool, len(paths))
	for _, path := range paths {
		want[path] = true
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for len(want) > 0 {
		select {
		case got := <-events:
			delete(want, got)
		case <-timer.C:
			t.Fatalf("no watcher notification for %v", want)
		}
	}
}

func inotifyInodes(t *testing.T) map[uint64]bool {
	t.Helper()
	result := make(map[uint64]bool)
	files, err := filepath.Glob("/proc/self/fdinfo/*")
	require.NoError(t, err)
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(line, "inotify ") {
				continue
			}
			for _, field := range strings.Fields(line) {
				if strings.HasPrefix(field, "ino:") {
					inode, err := strconv.ParseUint(strings.TrimPrefix(field, "ino:"), 16, 64)
					require.NoError(t, err)
					result[inode] = true
				}
			}
		}
	}
	return result
}
