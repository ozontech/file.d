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
	k8s_meta "github.com/ozontech/file.d/plugin/input/k8s/meta"
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

func (i *inputerMock) In(_ pipeline.SourceID, _ string, _ int64, data []byte, _ bool, _ metadata.MetaData) uint64 {
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

func TestNewMetaInformation(t *testing.T) {
	tests := []struct {
		name            string
		filename        string
		symlink         string
		inode           inodeID
		offset          int64
		parseK8sMeta    bool
		expectError     bool
		expectedK8sMeta *k8s_meta.K8sMetaInformation
	}{
		{
			name:         "Valid filename with K8s metadata",
			filename:     "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			symlink:      "",
			inode:        12345,
			offset:       0,
			parseK8sMeta: true,
			expectError:  false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{
				PodName:       "advanced-logs-checker-2222222222-trtrq",
				Namespace:     "sre",
				ContainerName: "duty-bot",
				ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
			},
		},
		{
			name:         "Valid symlink with K8s metadata",
			filename:     "/4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			symlink:      "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			inode:        12345,
			offset:       0,
			parseK8sMeta: true,
			expectError:  false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{
				PodName:       "advanced-logs-checker-2222222222-trtrq",
				Namespace:     "sre",
				ContainerName: "duty-bot",
				ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
			},
		},
		{
			name:            "Filename without k8s parse",
			filename:        "/4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			symlink:         "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			inode:           12345,
			offset:          0,
			parseK8sMeta:    false,
			expectError:     false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{},
		},
		{
			name:            "Invalid inputs without K8s metadata",
			filename:        "",
			symlink:         "",
			inode:           0,
			offset:          0,
			parseK8sMeta:    false,
			expectError:     false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{}, // No K8s metadata expected
		},
		{
			name:            "Invalid K8s metadata parsing",
			filename:        "invalidfile.txt",
			symlink:         "invalidsymlink",
			inode:           0,
			offset:          0,
			parseK8sMeta:    true,
			expectError:     true,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{}, // No K8s metadata expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaInfo, err := newMetaInformation(tt.filename, tt.symlink, tt.inode, tt.offset, tt.parseK8sMeta)

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}

			if !tt.expectError {
				assert.Equal(t, tt.filename, metaInfo.filename)
				assert.Equal(t, tt.symlink, metaInfo.symlink)
				assert.Equal(t, uint64(tt.inode), metaInfo.inode)
				assert.Equal(t, tt.offset, metaInfo.offset)

				if tt.parseK8sMeta {
					assert.Equal(t, tt.expectedK8sMeta.PodName, metaInfo.k8sMetadata.PodName)
					assert.Equal(t, tt.expectedK8sMeta.ContainerID, metaInfo.k8sMetadata.ContainerID)
					assert.Equal(t, tt.expectedK8sMeta.ContainerName, metaInfo.k8sMetadata.ContainerName)
					assert.Equal(t, tt.expectedK8sMeta.Namespace, metaInfo.k8sMetadata.Namespace)
				} else {
					assert.Equal(t, tt.expectedK8sMeta, metaInfo.k8sMetadata)
				}
			}
		})
	}
}

func TestGetData(t *testing.T) {
	tests := []struct {
		name     string
		metaInfo metaInformation
		expected map[string]any
	}{
		{
			name: "Valid data",
			metaInfo: metaInformation{
				filename: "/4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				symlink:  "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				inode:    12345,
				offset:   0,
				k8sMetadata: &k8s_meta.K8sMetaInformation{
					PodName:       "advanced-logs-checker-2222222222-trtrq",
					Namespace:     "sre",
					ContainerName: "duty-bot",
					ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
				},
			},
			expected: map[string]any{
				"filename":     "/4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				"symlink":      "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				"inode":        uint64(12345),
				"offset":       int64(0),
				"pod":          "advanced-logs-checker-2222222222-trtrq",
				"namespace":    "sre",
				"container":    "duty-bot",
				"container_id": "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.metaInfo.GetData()
			for key, expectedValue := range tt.expected {
				if data[key] != expectedValue {
					t.Errorf("expected %s: %v, got: %v", key, expectedValue, data[key])
				}
			}
		})
	}
}
