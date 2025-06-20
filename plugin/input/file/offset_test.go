package file

import (
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestParseOffsets(t *testing.T) {
	data := `- file: /some/informational/name
  inode: 1
  source_id: 1234
  streams:
    default: 100
    another: 200
- file: /another/informational/name
  inode: 2
  source_id: 4321
  streams:
    stderr: 300
`
	ctl := metric.NewCtl("test", prometheus.NewRegistry())
	metrics := newOffsetDbMetricCollection(
		ctl.RegisterCounter("worker1", "help_test"),
	)
	offsetDB := newOffsetDB("", "", metrics)
	offsets, err := offsetDB.parse(data)
	require.NoError(t, err)

	item, has := offsets[pipeline.SourceID(1234)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/some/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(1234), item.sourceID)

	offset, has := item.streams["default"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(100), offset, "wrong offset")

	offset, has = item.streams["another"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(200), offset, "wrong offset")

	item, has = offsets[pipeline.SourceID(4321)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/another/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(4321), item.sourceID)

	offset, has = item.streams["stderr"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(300), offset, "wrong offset")
}

func TestParallelOffsetsSave(t *testing.T) {
	data := `- file: /some/informational/name
  inode: 1
  source_id: 1234
  streams:
    default: 100
    another: 200
- file: /another/informational/name
  inode: 2
  source_id: 4321
  streams:
    stderr: 300
`
	jobs := make(map[pipeline.SourceID]*Job)
	offsets := pipeline.SliceFromMap(map[pipeline.StreamName]int64{
		"stdout": 111,
		"stderr": 222,
	})

	jobs[0] = &Job{
		file:           nil,
		inode:          2343,
		sourceID:       0,
		filename:       "/file1",
		symlink:        "/file1_sym",
		ignoreEventsLE: 0,
		lastEventSeq:   0,
		isVirgin:       false,
		isDone:         false,
		shouldSkip:     *atomic.NewBool(false),
		offsets:        offsets,
		mu:             &sync.Mutex{},
	}
	jobs[1] = &Job{
		file:           nil,
		inode:          233,
		sourceID:       0,
		filename:       "/file2",
		symlink:        "/file2_sym",
		ignoreEventsLE: 0,
		lastEventSeq:   0,
		isVirgin:       false,
		isDone:         false,
		shouldSkip:     *atomic.NewBool(false),
		offsets:        offsets,
		mu:             &sync.Mutex{},
	}
	rwmu := &sync.RWMutex{}
	count := 100
	wg := sync.WaitGroup{}
	wg.Add(count)
	ctl := metric.NewCtl("test", prometheus.NewRegistry())
	for range count {
		metrics := newOffsetDbMetricCollection(
			ctl.RegisterCounter("worker1", "help_test"),
		)
		go func() {
			offsetDB := newOffsetDB("tests-offsets", "tests-offsets.tmp", metrics)
			_, _ = offsetDB.parse(data)
			offsetDB.save(jobs, rwmu)
			wg.Done()
		}()
	}
	wg.Wait()
	err := os.Remove("tests-offsets")
	require.NoError(t, err)
}

func TestParallelOffsetsGetSet(t *testing.T) {
	offsets := pipeline.SliceFromMap(map[pipeline.StreamName]int64{
		"stdout": 111,
		"stderr": 222,
	})

	job := &Job{
		offsets: offsets,
		mu:      &sync.Mutex{},
	}

	count := 100
	wg := sync.WaitGroup{}
	wg.Add(2 * count)
	for range count {
		go func() {
			// like in `worker.work`
			job.mu.Lock()
			jobOffsets := job.offsets.Copy()
			job.mu.Unlock()
			o := pipeline.NewOffsets(0, jobOffsets)

			// like in `pipeline.In`
			_ = o.ByStream("test")

			wg.Done()
		}()

		go func() {
			// like in `jobProvider.commit`
			job.mu.Lock()
			job.offsets.Set("stderr", rand.Int63())
			job.mu.Unlock()

			wg.Done()
		}()
	}
	wg.Wait()
}
