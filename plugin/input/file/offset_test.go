package file

import (
	"os"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
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
	offsetDB := newOffsetDB("", "")
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

func TestParallel(t *testing.T) {
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
	wg.Add(100)
	for i := 0; i < count; i++ {
		go func() {
			offsetDB := newOffsetDB("tests-offsets", "tests-offsets.tmp")
			_, _ = offsetDB.parse(data)
			offsetDB.save(jobs, rwmu)
			wg.Done()
		}()
	}
	wg.Wait()
	err := os.Remove("tests-offsets")
	require.NoError(t, err)
}
