package matchrule

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRule(values []string, mode Mode, insensitive bool) Rule {
	r := Rule{
		Values:          values,
		Mode:            mode,
		CaseInsensitive: insensitive,
	}
	r.Prepare()
	return r
}

func TestRule_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		except Rule
		arg    []byte
		want   bool
	}{
		{
			name:   "prefix ok",
			except: newRule([]string{`{"level":"error"`}, ModePrefix, false),
			arg:    []byte(`{"level":"error","message":"some message"}`),
			want:   true,
		},
		{
			name:   "prefix not ok",
			except: newRule([]string{`{"level":"info"`}, ModePrefix, false),
			arg:    []byte(`{"level":"error","message":"some message"}`),
			want:   false,
		},
		{
			name:   "prefix ignore case",
			except: newRule([]string{`{"level":"info"`}, ModePrefix, true),
			arg:    []byte(`{"level":"INFO","message":"some message"}`),
			want:   true,
		},
		{
			name:   "suffix ok",
			except: newRule([]string{`"level":"error"}`}, ModeSuffix, false),
			arg:    []byte(`{"message":"some message","level":"error"}`),
			want:   true,
		},
		{
			name:   "suffix not ok",
			except: newRule([]string{`{"level":"info"`}, ModeSuffix, false),
			arg:    []byte(`{"message":"some message","level":"error"}`),
			want:   false,
		},
		{
			name:   "suffix ignore case",
			except: newRule([]string{`"level":"Info"}`}, ModeSuffix, true),
			arg:    []byte(`{"message":"some message","level":"INFO"}`),
			want:   true,
		},
		{
			name:   "contains ok",
			except: newRule([]string{`"level":"panic"`}, ModeContains, false),
			arg:    []byte(`{"time":"18:00", "event":"dinner", "level":"panic", "ok":"google"}`),
			want:   true,
		},
		{
			name:   "contains not ok",
			except: newRule([]string{`"level":"fatal"`}, ModeContains, false),
			arg:    []byte(`{"time":"18:00", "event":"dinner", "level":"panic", "ok":"google"}`),
			want:   false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			check := assert.True
			if !tt.want {
				check = assert.False
			}

			check(t, tt.except.Match(tt.arg))
		})
	}
}

const rawEvent = `{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`

func TestRule_Match_ZeroAlloc(t *testing.T) {
	rule := newRule(
		[]string{`"ts":"2019-08-21T11:43:25.865Z"`, `get_items_error_1`, `"trace_id":"3ea4a6589d06bb3f"`},
		ModeContains,
		false,
	)
	{
		allocs := testing.AllocsPerRun(10, func() {
			require.True(t, rule.Match([]byte(rawEvent)))
		})
		assert.Equal(t, 0.0, allocs)
	}

	rule.Values = []string{`{"level":"error"`}
	rule.Mode = ModePrefix
	{
		allocs := testing.AllocsPerRun(10, func() {
			e := newRule([]string{`{"level":"error"`}, ModePrefix, false)
			assert.True(t, e.Match([]byte(rawEvent)))
		})
		assert.Equal(t, 0.0, allocs)
	}

	rule.CaseInsensitive = true
	{
		allocs := testing.AllocsPerRun(10, func() {
			assert.True(t, rule.Match([]byte(rawEvent)))
		})
		assert.Equal(t, 1.0, allocs) // ToLower requires memory allocation
	}
}

func BenchmarkRule_Match(b *testing.B) {
	for _, insensitive := range []bool{false, true} {
		b.Run(fmt.Sprintf("prefix_%v", insensitive), func(b *testing.B) {
			e := newRule([]string{`{"level":"error"`}, ModePrefix, insensitive)
			runMatchBench(b, &e)
		})

		b.Run(fmt.Sprintf("suffix_%v", insensitive), func(b *testing.B) {
			e := newRule([]string{`"context_canceled"}`}, ModeSuffix, insensitive)
			runMatchBench(b, &e)
		})
	}

	b.Run("contains", func(b *testing.B) {
		e := newRule([]string{`get_items_error_1`}, ModeContains, false)
		runMatchBench(b, &e)
	})
}

func runMatchBench(b *testing.B, e *Rule) {
	for i := 0; i < b.N; i++ {
		if !e.Match([]byte(rawEvent)) {
			b.Fatalf("value should match")
		}
	}
}

func TestException_Match(t *testing.T) {
	e := RuleSet{
		Cond: CondAnd,
		Rules: []Rule{
			newRule([]string{"404"}, ModePrefix, true),
			newRule([]string{"ok"}, ModePrefix, true),
		},
	}
	require.False(t, e.Match([]byte("ok")))

	e.Cond = CondOr

	require.True(t, e.Match([]byte("ok")))
}
