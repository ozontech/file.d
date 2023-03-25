package antispam

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestException_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		except Exception
		arg    []byte
		want   bool
	}{
		{
			name:   "prefix ok",
			except: NewException(`{"level":"error"`, ConditionPrefix, false),
			arg:    []byte(`{"level":"error","message":"some message"}`),
			want:   true,
		},
		{
			name:   "prefix not ok",
			except: NewException(`{"level":"info"`, ConditionPrefix, false),
			arg:    []byte(`{"level":"error","message":"some message"}`),
			want:   false,
		},
		{
			name:   "prefix ignore case",
			except: NewException(`{"level":"info"`, ConditionPrefix, true),
			arg:    []byte(`{"level":"INFO","message":"some message"}`),
			want:   true,
		},
		{
			name:   "suffix ok",
			except: NewException(`"level":"error"}`, ConditionSuffix, false),
			arg:    []byte(`{"message":"some message","level":"error"}`),
			want:   true,
		},
		{
			name:   "suffix not ok",
			except: NewException(`{"level":"info"`, ConditionSuffix, false),
			arg:    []byte(`{"message":"some message","level":"error"}`),
			want:   false,
		},
		{
			name:   "suffix ignore case",
			except: NewException(`"level":"Info"}`, ConditionSuffix, true),
			arg:    []byte(`{"message":"some message","level":"INFO"}`),
			want:   true,
		},
		{
			name:   "contains ok",
			except: NewException(`"level":"panic"`, ConditionContains, false),
			arg:    []byte(`{"time":"18:00", "event":"dinner", "level":"panic", "ok":"google"}`),
			want:   true,
		},
		{
			name:   "contains not ok",
			except: NewException(`"level":"fatal"`, ConditionContains, false),
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

func TestException_Match_ZeroAlloc(t *testing.T) {
	allocs := testing.AllocsPerRun(10, func() {
		e := NewException(`"ts":"2019-08-21T11:43:25.865Z"`, ConditionContains, false)
		require.True(t, e.Match([]byte(rawEvent)))
	})
	assert.Equal(t, 0.0, allocs)

	allocs = testing.AllocsPerRun(10, func() {
		e := NewException(`{"level":"error"`, ConditionPrefix, true)
		assert.True(t, e.Match([]byte(rawEvent)))
	})
	assert.Equal(t, 1.0, allocs) // ToLower requires memory allocation
}

func BenchmarkException_Match(b *testing.B) {
	for _, insensitive := range []bool{false, true} {
		b.Run(fmt.Sprintf("prefix_%v", insensitive), func(b *testing.B) {
			e := NewException(`{"level":"error"`, ConditionPrefix, insensitive)
			runMatchBench(b, &e)
		})

		b.Run(fmt.Sprintf("suffix_%v", insensitive), func(b *testing.B) {
			e := NewException(`"context_canceled"}`, ConditionSuffix, insensitive)
			runMatchBench(b, &e)
		})
	}

	b.Run("contains", func(b *testing.B) {
		e := NewException(`get_items_error_1`, ConditionContains, false)
		runMatchBench(b, &e)
	})
}

func runMatchBench(b *testing.B, e *Exception) {
	for i := 0; i < b.N; i++ {
		if !e.Match([]byte(rawEvent)) {
			b.Fatalf("value should match")
		}
	}
}
