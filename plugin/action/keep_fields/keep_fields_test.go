package keep_fields

import (
	"runtime/debug"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
)

func TestKeepFields(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"field_1", "field_2"}}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", test.NewOffset(0), []byte(`{"field_3":"value_3","a":"b"}`))

	wg.Wait()
	p.Stop()

	require.Equal(t, 3, len(outEvents), "wrong out events count")
	require.Equal(t, `{"field_1":"value_1"}`, outEvents[0], "wrong event")
	require.Equal(t, `{"field_2":"value_2"}`, outEvents[1], "wrong event")
	require.Equal(t, `{}`, outEvents[2], "wrong event")
}

func TestKeepNestedFields(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"a.b.c", "a.b.d", "a.d", "f"}}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.NewOffset(0),
		[]byte(`{"a":{"b":{"c":1,"d":1}},"d":1}`),
	)
	input.In(0, "test.log", test.NewOffset(0),
		[]byte(`{"a":{"b":[1,2],"d":1}}`),
	)
	input.In(0, "test.log", test.NewOffset(0),
		[]byte(`{"a":{"g":"h","i":"j","f":"nested"},"f":"k"}`),
	)

	wg.Wait()
	p.Stop()

	require.Equal(t, 3, len(outEvents), "wrong out events count")
	require.Equal(t, `{"a":{"b":{"c":1,"d":1}}}`, outEvents[0], "wrong event")
	require.Equal(t, `{"a":{"d":1}}`, outEvents[1], "wrong event")
	require.Equal(t, `{"f":"k"}`, outEvents[2], "wrong event")
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoFlatNoFieldsSaved(b *testing.B) {
	fields := getFlatConfig()
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("old", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoOld(a[i])
		}
	})
	b.Run("tree_slow", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeSlow(a[i])
		}
	})
	b.Run("tree_fast", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeFast(a[i])
		}
	})
	b.Run("array_slow", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArraySlow(a[i])
		}
	})
	b.Run("array_fast", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArrayFast(a[i])
		}
	})
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoFlatHalfFieldsSaved(b *testing.B) {
	fields := getFlatConfig()
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("old", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoOld(a[i])
		}
	})
	b.Run("tree_slow", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeSlow(a[i])
		}
	})
	b.Run("tree_fast", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeFast(a[i])
		}
	})
	b.Run("array_slow", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArraySlow(a[i])
		}
	})
	b.Run("array_fast", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArrayFast(a[i])
		}
	})
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoFlatAllFieldsSaved(b *testing.B) {
	fields := getFlatConfig()
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("old", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoOld(a[i])
		}
	})
	b.Run("array_slow", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArraySlow(a[i])
		}
	})
	b.Run("array_fast", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArrayFast(a[i])
		}
	})
	b.Run("tree_slow", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeSlow(a[i])
		}
	})
	b.Run("tree_fast", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeFast(a[i])
		}
	})
}
