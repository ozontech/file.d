package file

import (
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtime"
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
    error:: 960
    another: 200
- file: /another/informational/name
  inode: 2
  source_id: 4321
  last_read_timestamp: 1763651665000000000
  streams:
    stderr: 300
    error:: 0
`
	offsetDB := newOffsetDB("", "")
	offsets, err := offsetDB.parse(data)
	require.NoError(t, err)

	item, has := offsets[pipeline.SourceID(1234)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/some/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(1234), item.sourceID)
	assert.InDelta(t, xtime.GetInaccurateUnixNano(), item.lastReadTimestamp, 10)

	offset, has := item.streams["default"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(100), offset, "wrong offset")

	offset, has = item.streams["error:"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(960), offset, "wrong offset")

	offset, has = item.streams["another"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(200), offset, "wrong offset")

	item, has = offsets[pipeline.SourceID(4321)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/another/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(4321), item.sourceID)
	assert.Equal(t, int64(1763651665000000000), item.lastReadTimestamp)

	offset, has = item.streams["stderr"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(300), offset, "wrong offset")

	offset, has = item.streams["error:"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(0), offset, "wrong offset")
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
	for range count {
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

func TestParseLine(t *testing.T) {
	offsetDB := newOffsetDB("", "")

	tests := []struct {
		name           string
		content        string
		prefix         string
		expectedLine   string
		expectedRemain string
		expectedErr    string
	}{
		// happy path
		{
			name:           "parse file line",
			content:        "- file: /some/informational/name\n  inode: 1",
			prefix:         "- file: ",
			expectedLine:   "/some/informational/name",
			expectedRemain: "  inode: 1",
		},
		{
			name:           "parse indented file line",
			content:        "  file: /some/informational/name\n  inode: 1",
			prefix:         "  file: ",
			expectedLine:   "/some/informational/name",
			expectedRemain: "  inode: 1",
		},
		{
			name:           "parse inode line",
			content:        "  inode: 1\n  source_id: 1234",
			prefix:         "  inode: ",
			expectedLine:   "1",
			expectedRemain: "  source_id: 1234",
		},
		{
			name:           "parse source_id line",
			content:        "  source_id: 1234\n  streams:\n    default: 100",
			prefix:         "  source_id: ",
			expectedLine:   "1234",
			expectedRemain: "  streams:\n    default: 100",
		},
		{
			name:           "parse streams header",
			content:        "  streams:\n    default: 100\n    error:: 960",
			prefix:         "  streams:",
			expectedLine:   "",
			expectedRemain: "    default: 100\n    error:: 960",
		},
		{
			name:           "parse stream line with double colon",
			content:        "    error:: 960\n    another: 200",
			prefix:         "    error:: ",
			expectedLine:   "960",
			expectedRemain: "    another: 200",
		},
		{
			name:           "parse stream line with single colon",
			content:        "    default: 100\n    error:: 960",
			prefix:         "    default: ",
			expectedLine:   "100",
			expectedRemain: "    error:: 960",
		},
		{
			name:           "parse zero value stream",
			content:        "    error:: 0\n- file: /another/file",
			prefix:         "    error:: ",
			expectedLine:   "0",
			expectedRemain: "- file: /another/file",
		},
		{
			name:           "parse empty stream value",
			content:        "    empty:\n    next: 100",
			prefix:         "    empty:",
			expectedLine:   "",
			expectedRemain: "    next: 100",
		},
		{
			name:           "line with trailing spaces",
			content:        "  inode: 1  \n  source_id: 1234",
			prefix:         "  inode: ",
			expectedLine:   "1  ",
			expectedRemain: "  source_id: 1234",
		},

		// Error cases
		{
			name:        "empty content",
			content:     "",
			prefix:      "  file: ",
			expectedErr: "unexpected end of content while looking for",
		},
		{
			name:        "no newline",
			content:     "  file: /some/path",
			prefix:      "  file: ",
			expectedErr: "no newline found in content",
		},
		{
			name:        "wrong prefix",
			content:     "  filename: /some/path\n",
			prefix:      "  file: ",
			expectedErr: "expected prefix",
		},
		{
			name:        "prefix longer than line",
			content:     "  fil\n",
			prefix:      "  file: ",
			expectedErr: "expected prefix",
		},
		{
			name:        "wrong indentation",
			content:     " file: /some/path\n", // one space instead of two
			prefix:      "  file: ",
			expectedErr: "expected prefix",
		},
		{
			name:        "missing colon",
			content:     "  file /some/path\n",
			prefix:      "  file: ",
			expectedErr: "expected prefix",
		},
		{
			name:        "case mismatch",
			content:     "  File: /some/path\n",
			prefix:      "  file: ",
			expectedErr: "expected prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, remain, err := offsetDB.parseLine(tt.content, tt.prefix)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedLine, line)
			assert.Equal(t, tt.expectedRemain, remain)
		})
	}
}

func TestParseOptionalLineLastReadTimestamp(t *testing.T) {
	offsetDB := newOffsetDB("", "")

	tests := []struct {
		name           string
		content        string
		expectedLine   string
		expectedRemain string
		expectedErr    bool
		desc           string
	}{
		// last_read_timestamp exists
		{
			name:           "has last_read_timestamp before streams",
			content:        "  last_read_timestamp: 1763651665000000000\n  streams:\n    default: 100",
			expectedLine:   "1763651665000000000",
			expectedRemain: "  streams:\n    default: 100",
			desc:           "timestamp present, streams follows",
		},
		{
			name:           "has last_read_timestamp before new item",
			content:        "  last_read_timestamp: 1763651665000000000\n- file: /another/file",
			expectedLine:   "1763651665000000000",
			expectedRemain: "- file: /another/file",
			desc:           "timestamp present, new item follows",
		},
		{
			name:           "has last_read_timestamp at end",
			content:        "  last_read_timestamp: 1763651665000000000\n",
			expectedLine:   "1763651665000000000",
			expectedRemain: "",
			desc:           "timestamp present at end of content",
		},
		{
			name:           "has last_read_timestamp with spaces",
			content:        "  last_read_timestamp:  1763651665000000000  \n  streams:",
			expectedLine:   " 1763651665000000000  ",
			expectedRemain: "  streams:",
			desc:           "timestamp with extra spaces",
		},
		{
			name:           "has last_read_timestamp zero value",
			content:        "  last_read_timestamp: 0\n  streams:",
			expectedLine:   "0",
			expectedRemain: "  streams:",
			desc:           "zero timestamp value",
		},

		// last_read_timestamp does NOT exist
		{
			name:           "no last_read_timestamp, streams follows",
			content:        "  streams:\n    default: 100",
			expectedLine:   "",
			expectedRemain: "  streams:\n    default: 100",
			desc:           "no timestamp, streams follows",
		},
		{
			name:           "no last_read_timestamp, new item starts",
			content:        "- file: /another/file\n  inode: 2",
			expectedLine:   "",
			expectedRemain: "- file: /another/file\n  inode: 2",
			desc:           "no timestamp, new item starts",
		},
		{
			name:           "no last_read_timestamp, empty content",
			content:        "",
			expectedLine:   "",
			expectedRemain: "",
			desc:           "empty content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			line, remain, err := offsetDB.parseOptionalLine(tt.content, "  last_read_timestamp: ")

			if tt.expectedErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unexpected line format")
				assert.Contains(t, err.Error(), "expected \"  last_read_timestamp: \"")
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedLine, line, "timestamp value mismatch")
			assert.Equal(t, tt.expectedRemain, remain, "remaining content mismatch")
		})
	}
}
