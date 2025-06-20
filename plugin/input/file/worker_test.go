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

func (i *inputerMock) IncMaxEventSizeExceeded(lvs ...string) {}

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
		name               string
		maxEventSize       int
		readBufferSize     int
		cutOffEventByLimit bool

		inFile  string
		expData string
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
		{
			name:               "should_ok_and_cutoff_long_line",
			maxEventSize:       2,
			cutOffEventByLimit: true,
			inFile:             "abc\n",
			readBufferSize:     1024,
			expData:            "abc\n",
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
				ctl.RegisterCounter("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
				ctl.RegisterGauge("worker5", "help_test"),
			)
			jp := NewJobProvider(&Config{}, metrics, &zap.SugaredLogger{})
			jp.jobsChan = make(chan *Job, 2)
			jp.jobs = map[pipeline.SourceID]*Job{
				1: job,
			}
			jp.jobsChan <- job
			jp.jobsChan <- nil

			w := &worker{
				maxEventSize:       tt.maxEventSize,
				cutOffEventByLimit: tt.cutOffEventByLimit,
			}
			inputer := inputerMock{}

			w.work(&inputer, jp, tt.readBufferSize, zap.L().Sugar().With("fd"))

			assert.Equal(t, tt.expData, inputer.lastData())
		})
	}
}

func TestWorkerWorkMultiData(t *testing.T) {
	tests := []struct {
		name               string
		maxEventSize       int
		readBufferSize     int
		cutOffEventByLimit bool

		inData  string
		outData []string
	}{
		{
			name:           "long_event",
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
			name:           "long_event_begin_file",
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
			name:           "long_event_end_file",
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
			name:               "long_event_cutoff",
			maxEventSize:       50,
			readBufferSize:     1024,
			cutOffEventByLimit: true,

			inData: fmt.Sprintf(`{"a":"a"}
{"key":"%s"}
{"a":"a"}
{"key":"%s"}
{"a":"a"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				fmt.Sprintf(`{"key":"%s"}`, strings.Repeat("a", 50)) + "\n",
				`{"a":"a"}` + "\n",
				fmt.Sprintf(`{"key":"%s"}`, strings.Repeat("a", 50)) + "\n",
				`{"a":"a"}` + "\n",
			},
		},
		{
			name:           "small_buffer",
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
			name:               "small_buffer_cutoff",
			maxEventSize:       50,
			readBufferSize:     5,
			cutOffEventByLimit: true,

			inData: fmt.Sprintf(`{"a":"a"}
{"a":"a"}
{"key":"%s"}
{"a":"a"}
{"key":"%s"}
`, strings.Repeat("a", 50), strings.Repeat("a", 50)),
			outData: []string{
				`{"a":"a"}` + "\n",
				`{"a":"a"}` + "\n",
				fmt.Sprintf(`{"key":"%s"}`, strings.Repeat("a", 45)) + "\n",
				`{"a":"a"}` + "\n",
				fmt.Sprintf(`{"key":"%s"}`, strings.Repeat("a", 46)) + "\n",
			},
		},
		{
			name:           "no_new_line",
			maxEventSize:   50,
			readBufferSize: 1024,

			inData:  `{"a":"a"}`,
			outData: nil, // we don't send an event if we don't find a newline
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &worker{
				maxEventSize:       tt.maxEventSize,
				cutOffEventByLimit: tt.cutOffEventByLimit,
			}

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
				offsets:    pipeline.SliceMap{},
				mu:         &sync.Mutex{},
			}

			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			metrics := newMetricCollection(
				ctl.RegisterCounter("worker1", "help_test"),
				ctl.RegisterCounter("worker2", "help_test"),
				ctl.RegisterCounter("worker3", "help_test"),
				ctl.RegisterGauge("worker4", "help_test"),
				ctl.RegisterGauge("worker5", "help_test"),
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
	k8s_meta.DisableMetaUpdates = true
	tests := []struct {
		name            string
		filename        string
		symlink         string
		inode           inodeID
		parseK8sMeta    bool
		expectError     bool
		expectedK8sMeta *k8s_meta.K8sMetaInformation
	}{
		{
			name:         "Valid filename with K8s metadata",
			filename:     "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
			symlink:      "",
			inode:        12345,
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
			parseK8sMeta:    false,
			expectError:     false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{},
		},
		{
			name:            "Invalid inputs without K8s metadata",
			filename:        "",
			symlink:         "",
			inode:           0,
			parseK8sMeta:    false,
			expectError:     false,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{}, // No K8s metadata expected
		},
		{
			name:            "Invalid K8s metadata parsing",
			filename:        "invalidfile.txt",
			symlink:         "invalidsymlink",
			inode:           0,
			parseK8sMeta:    true,
			expectError:     true,
			expectedK8sMeta: &k8s_meta.K8sMetaInformation{}, // No K8s metadata expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaInfo, err := newMetaInformation(tt.filename, tt.symlink, tt.inode, tt.parseK8sMeta)

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}

			if !tt.expectError {
				assert.Equal(t, tt.filename, metaInfo.filename)
				assert.Equal(t, tt.symlink, metaInfo.symlink)
				assert.Equal(t, uint64(tt.inode), metaInfo.inode)

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
				k8sMetadata: &k8s_meta.K8sMetaInformation{
					PodName:       "advanced-logs-checker-2222222222-trtrq",
					Namespace:     "sre",
					ContainerName: "duty-bot",
					ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
				},
			},
			expected: map[string]any{
				"filename":       "/4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				"symlink":        "/k8s-logs/advanced-logs-checker-2222222222-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log",
				"inode":          uint64(12345),
				"pod_name":       "advanced-logs-checker-2222222222-trtrq",
				"namespace":      "sre",
				"container_name": "duty-bot",
				"container_id":   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
			},
		},
		{
			name: "No k8s data",
			metaInfo: metaInformation{
				filename: "/container.log",
				symlink:  "/k8s-logs/container.log",
				inode:    12345,
			},
			expected: map[string]any{
				"filename":       "/container.log",
				"symlink":        "/k8s-logs/container.log",
				"inode":          uint64(12345),
				"pod_name":       nil,
				"namespace":      nil,
				"container_name": nil,
				"container_id":   nil,
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
