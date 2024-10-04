package file

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type inputerMock struct {
	gotData []string
}

func (i *inputerMock) IncReadOps() {}

func (i *inputerMock) IncMaxEventSizeExceeded() {}

func (i *inputerMock) In(_ pipeline.SourceID, _ string, _ pipeline.Offsets, data []byte, _ bool, _ metadata.MetaData) uint64 {
	i.gotData = append(i.gotData, string(data))
	return 0
}

func (i *inputerMock) lastData() string {
	if len(i.gotData)-1 < 0 {
		return ""
	}
	return i.gotData[len(i.gotData)-1]
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
			f, err := os.CreateTemp("/tmp", "worker_test")
			require.NoError(t, err)
			info, err := f.Stat()
			require.NoError(t, err)
			defer os.Remove(path.Join("/tmp", info.Name()))

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
				mu:             &sync.Mutex{},
			}
			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			metrics := newMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
				ctl.RegisterCounter("worker2", "help_test"),
				ctl.RegisterGauge("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
			)
			jp := NewJobProvider(&Config{}, metrics, &zap.SugaredLogger{})
			jp.jobsChan = make(chan *Job, 2)
			jp.jobs = map[pipeline.SourceID]*Job{
				1: job,
			}
			jp.jobsChan <- job
			jp.jobsChan <- nil

			w := &worker{maxEventSize: tt.maxEventSize}
			inputer := inputerMock{}

			w.work(&inputer, jp, tt.readBufferSize, zap.L().Sugar().With("fd"))

			assert.Equal(t, tt.expData, inputer.lastData())
		})
	}
}

func TestWorkerWorkMultiData(t *testing.T) {
	tests := []struct {
		name           string
		maxEventSize   int
		readBufferSize int

		inData  string
		outData []string
	}{
		{
			name:           "long event",
			maxEventSize:   50,
			readBufferSize: 1024,

			inData: fmt.Sprintf(`{"a":"a"}
{"key":"%s"}
{"a":"a"}
{"key":"%s"}
{"a":"a"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
			},
		},
		{
			name:           "long event at the beginning of the file",
			maxEventSize:   50,
			readBufferSize: 1024,

			inData: fmt.Sprintf(`{"key":"%s"}
{"a":"a"}
{"a":"a"}
{"key":"%s"}
{"a":"a"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
			},
		},
		{
			name:           "long event at the end of the file",
			maxEventSize:   50,
			readBufferSize: 1024,

			inData: fmt.Sprintf(`{"a":"a"}
{"a":"a"}
{"a":"a"}
{"key":"%s"}
{"key":"%s"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
			},
		},
		{
			name:           "small buffer",
			maxEventSize:   50,
			readBufferSize: 5,

			inData: fmt.Sprintf(`{"a":"a"}
{"a":"a"}
{"key":"%s"}
{"a":"a"}
{"key":"%s"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
			},
		},
		{
			name:           "no new line",
			maxEventSize:   50,
			readBufferSize: 1024,

			inData:  `{"a":"a"}`,
			outData: nil, // we don't send an event if we don't find a newline
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{maxEventSize: tt.maxEventSize}

			f, err := os.CreateTemp("/tmp", "worker_test")
			require.NoError(t, err)
			info, err := f.Stat()
			require.NoError(t, err)
			defer os.Remove(path.Join("/tmp", info.Name()))

			_, _ = fmt.Fprint(f, tt.inData)

			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)

			job := &Job{
				file:       f,
				shouldSkip: *atomic.NewBool(false),
				offsets:    sliceMap{},
				mu:         &sync.Mutex{},
			}

			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			metrics := newMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
				ctl.RegisterCounter("worker2", "help_test"),
				ctl.RegisterGauge("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
			)
			jp := NewJobProvider(&Config{}, metrics, &zap.SugaredLogger{})
			jp.jobsChan = make(chan *Job, 2)
			jp.jobs = map[pipeline.SourceID]*Job{
				1: job,
			}
			jp.jobsChan <- job
			jp.jobsChan <- nil

			inputer := inputerMock{}
			w.work(&inputer, jp, tt.readBufferSize, zap.L().Sugar().With("fd"))

			assert.Equal(t, tt.outData, inputer.gotData)
		})
	}
}
