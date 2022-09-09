package file

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rjeczalik/notify"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestWatcher(t *testing.T) {
	tests := []struct {
		name            string
		filenamePattern string
		dirPattern      string
	}{
		{
			name:            "should_ok_and_count_only_creation",
			filenamePattern: "watch*.log",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			shouldCreate := atomic.Int64{}
			notifyFn := func(_ notify.Event, _ string, _ os.FileInfo) {
				shouldCreate.Inc()
			}
			w := NewWatcher(path, tt.filenamePattern, tt.dirPattern, notifyFn, false, zap.L().Sugar())
			w.start()
			defer w.stop()

			// create, write, remove files and ensure events are only passed for creation events.

			f1Name := filepath.Join(path, "watch1.log")

			f1, err := os.Create(f1Name)
			require.NoError(t, err)
			err = f1.Close()
			require.NoError(t, err)

			f2, err := os.Create(filepath.Join(path, "watch2.log"))
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
