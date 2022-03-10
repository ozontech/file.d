package file

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type inputerMock struct {
	gotData []byte
}

func (i *inputerMock) In(sourceID pipeline.SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64 {
	i.gotData = make([]byte, len(data))
	copy(i.gotData, data)
	return 0
}

func TestWorkerWork(t *testing.T) {
	tests := []struct {
		name           string
		maxEventSize   int
		inFile         string
		readBufferSize int
		expData        string
	}{
		{
			name:           "should_ok_when_read_1_line",
			maxEventSize:   1024,
			inFile:         "abc\n",
			readBufferSize: 1024,
			expData:        "abc\n",
		},
		{
			name:           "should_ok_and_empty_when_read_not_ready_line",
			maxEventSize:   1024,
			inFile:         "abc",
			readBufferSize: 1024,
			expData:        "",
		},
		{
			name:           "should_ok_and_not_read_long_line",
			maxEventSize:   2,
			inFile:         "abc\n",
			readBufferSize: 1024,
			expData:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{maxEventSize: tt.maxEventSize}
			inputer := inputerMock{}
			f, err := os.CreateTemp("/tmp", "worker_test")
			require.NoError(t, err)
			_, _ = fmt.Fprint(f, tt.inFile)
			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)
			job := &Job{
				file:           f,
				inode:          0,
				sourceID:       0,
				filename:       "",
				symlink:        "",
				ignoreEventsLE: 0,
				lastEventSeq:   0,
				isVirgin:       false,
				isDone:         false,
				shouldSkip:     *atomic.NewBool(false),
				offsets:        sliceMap{},
				mu:             &sync.Mutex{},
			}
			jp := jobProvider{
				config:     &Config{},
				controller: nil,
				watcher:    &watcher{},
				offsetDB:   &offsetDB{},
				isStarted:  atomic.Bool{},
				jobs: map[pipeline.SourceID]*Job{
					1: job,
				},
				jobsMu:            &sync.RWMutex{},
				jobsChan:          make(chan *Job, 2),
				jobsLog:           []string{},
				symlinks:          map[inode]string{},
				symlinksMu:        &sync.Mutex{},
				jobsDone:          &atomic.Int32{},
				loadedOffsets:     map[pipeline.SourceID]*inodeOffsets{},
				stopSaveOffsetsCh: make(chan bool),
				stopReportCh:      make(chan bool),
				stopMaintenanceCh: make(chan bool),
				offsetsCommitted:  &atomic.Int64{},
				logger:            &zap.SugaredLogger{},
			}
			jp.jobsChan <- job
			jp.jobsChan <- nil

			w.work(&inputer, &jp, tt.readBufferSize, zap.L().Sugar().With("fd"))

			assert.Equal(t, tt.expData, string(inputer.gotData))
		})
	}
}
