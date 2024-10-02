package join

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

const contentPostgres = `# ===next===
2021-10-12 08:25:44 GMT [23379] => [520-1] client=[local],db=exampledb,user=none LOG:  duration: 0.287 ms  bind <unnamed>: select distinct connamespace as schema_id
	from pg_catalog.pg_constraint F,
	    pg_catalog.pg_class O
	where F.contype = 'f'
	 and F.confrelid = O.oid
	 and O.relnamespace in ($1)

# ===next===
2021-10-12 08:25:44 GMT [23379] => [521-1] client=[local],db=exampledb,user=none DETAIL:  parameters: $1 = '2200'

# ===next===
2021-10-12 08:25:44 GMT [23379] => [522-1] client=[local],db=exampledb,user=none LOG:  duration: 0.043 ms  execute <unnamed>: select distinct connamespace as schema_id
	from pg_catalog.pg_constraint F,
	    pg_catalog.pg_class O
	where F.contype = 'f'
	 and F.confrelid = O.oid
	 and O.relnamespace in ($1)

# ===next===
2021-10-12 08:25:44 GMT [23379] => [523-1] client=[local],db=exampledb,user=none DETAIL:  parameters: $1 = '2200'

# ===next===
2021-10-12 08:25:44 GMT [23379] => [524-1] client=[local],db=exampledb,user=none LOG:  duration: 0.056 ms  parse <unnamed>: SHOW TRANSACTION ISOLATION LEVEL

# ===next===
2021-10-12 08:25:44 GMT [23379] => [525-1] client=[local],db=exampledb,user=none LOG:  duration: 0.009 ms  bind <unnamed>: SHOW TRANSACTION ISOLATION LEVEL

# ===next===
2021-10-12 08:25:44 GMT [23379] => [526-1] client=[local],db=exampledb,user=none LOG:  duration: 0.018 ms  execute <unnamed>: SHOW TRANSACTION ISOLATION LEVEL
`

const contentPostgresWithNilNodes = `# ===next===
2021-10-12 08:25:44 GMT [23379] => [520-1] client=[local],db=exampledb,user=none LOG:  duration: 0.287 ms  bind <unnamed>: select distinct connamespace as schema_id
	from pg_catalog.pg_constraint F,
	    pg_catalog.pg_class O
	where F.contype = 'f'
NilNode:some message
NilNode:some message 2
	 and F.confrelid = O.oid
	 and O.relnamespace in ($1)
# ===next===
2021-10-12 08:25:44 GMT [23379] => [521-1] client=[local],db=exampledb,user=none DETAIL:  parameters: $1 = '2200'
# ===next===
2021-10-12 08:25:44 GMT [23379] => [522-1] client=[local],db=exampledb,user=none LOG:  duration: 0.043 ms  execute <unnamed>: select distinct connamespace as schema_id
	from pg_catalog.pg_constraint F,
	    pg_catalog.pg_class O
NilNode:some other message
	where F.contype = 'f'
	 and F.confrelid = O.oid
	 and O.relnamespace in ($1)
# ===next===
2021-10-12 08:25:44 GMT [23379] => [523-1] client=[local],db=exampledb,user=none DETAIL:  parameters: $1 = '2200'
# ===next===
2021-10-12 08:25:44 GMT [23379] => [524-1] client=[local],db=exampledb,user=none LOG:  duration: 0.056 ms  parse <unnamed>: SHOW TRANSACTION ISOLATION LEVEL
# ===next===
2021-10-12 08:25:44 GMT [23379] => [525-1] client=[local],db=exampledb,user=none LOG:  duration: 0.009 ms  bind <unnamed>: SHOW TRANSACTION ISOLATION LEVEL
# ===next===
2021-10-12 08:25:44 GMT [23379] => [526-1] client=[local],db=exampledb,user=none LOG:  duration: 0.018 ms  execute <unnamed>: SHOW TRANSACTION ISOLATION LEVEL`

const contentCustomWithNilNodes = `# ===next===
Start: event 1
Continue: event 1 cont
NilNode: event 2
Continue: event 3
# ===next===
Start: event 4

Continue: event 4 cont
# ===next===
Start: event 5

Continue: event 5 cont

NilNode: event 6
Continue: event 7
Start: event 8
`

func TestSimpleJoin(t *testing.T) {
	const startPattern = `/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.*?\[\d+\] => .+?client=.+?,db=.+?,user=.+:.*/`
	cases := []struct {
		name        string
		startPat    string
		continuePat string
		content     string
		expEvents   int32
		iterations  int
		negate      bool
	}{
		{
			name:        "should_ok_for_postgres_logs",
			startPat:    startPattern,
			continuePat: `/.+/`,
			content:     contentPostgres,
			iterations:  100,
			expEvents:   7 * 100,
		},
		{
			name:        "should_ok_for_postgres_logs_with_negate",
			startPat:    startPattern,
			continuePat: startPattern,
			content:     contentPostgres,
			iterations:  100,
			expEvents:   7 * 100,
			negate:      true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			format := `{"log":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if line == "" {
					continue
				}
				lines = append(lines, fmt.Sprintf(format, line))
			}

			config := test.NewConfig(&Config{
				Field:    "log",
				Start:    cfg.Regexp(tt.startPat),
				Continue: cfg.Regexp(tt.continuePat),
				Negate:   tt.negate,
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				if id != 0 && id >= e.SeqID {
					panic("wrong id")
				}
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.Offset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i     = 0
				iters = 100
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				break
			}

			p.Stop()

			require.True(t, iters > i, "test timed out")
			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		})
	}
}

func TestJoinAfterNilNode(t *testing.T) {
	cases := []struct {
		name        string
		startPat    string
		continuePat string
		content     string
		expEvents   int32
		iterations  int
	}{
		{
			name:        "should_ok_for_postgres_logs",
			startPat:    `/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.*?\[\d+\] => .+?client=.+?,db=.+?,user=.+:.*/`,
			continuePat: `/.+/`,
			content:     contentPostgresWithNilNodes,
			iterations:  100,
			expEvents:   15 * 100,
		},
		{
			name:        "should_ok_for_custom_logs",
			startPat:    `/^(?i)Start:/`,
			continuePat: `/(^\s*$)|(^(?i)Continue:)/`,
			content:     contentCustomWithNilNodes,
			iterations:  100,
			expEvents:   8 * 100,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			formatNode := `{"log":"%s\n"}`
			formatNilNode := `{"notlog":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if strings.HasPrefix(line, "NilNode:") {
					lines = append(lines, fmt.Sprintf(formatNilNode, line))
					continue
				}
				lines = append(lines, fmt.Sprintf(formatNode, line))
			}

			config := test.NewConfig(&Config{
				Field:    "log",
				Start:    cfg.Regexp(tt.startPat),
				Continue: cfg.Regexp(tt.continuePat),
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				if id != 0 && id >= e.SeqID {
					panic("wrong id")
				}
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.Offset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i     = 0
				iters = 100
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				break
			}

			p.Stop()

			require.True(t, iters > i, "test timed out")
			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		})
	}
}
