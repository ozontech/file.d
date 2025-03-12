package keep_fields

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
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
func BenchmarkDoFlatAllDeleted(b *testing.B) {
	config := &Config{Fields: getFlatConfig()}

	p := &Plugin{}

	p.Start(config, nil)

	const eventsPerIteration = 1

	debug.SetGCPercent(-1)

	b.Run("array_fast", func(b *testing.B) {
		n := b.N * eventsPerIteration
		a := getEventsAllFieldsDeleted(b, n)

		b.ResetTimer()

		from, to := 0, eventsPerIteration
		for i := 0; i < b.N; i++ {
			for _, event := range a[from:to] {
				p.DoNewWithArrayFast(event)
			}
			from += eventsPerIteration
			to += eventsPerIteration
		}
	})
	b.Run("old", func(b *testing.B) {
		n := b.N * eventsPerIteration
		a := getEventsAllFieldsDeleted(b, n)

		b.ResetTimer()

		from, to := 0, eventsPerIteration
		for i := 0; i < b.N; i++ {
			for _, event := range a[from:to] {
				p.DoOld(event)
			}
			from += eventsPerIteration
			to += eventsPerIteration
		}
	})
	b.Run("tree_slow", func(b *testing.B) {
		n := b.N * eventsPerIteration
		a := getEventsAllFieldsDeleted(b, n)

		b.ResetTimer()

		from, to := 0, eventsPerIteration
		for i := 0; i < b.N; i++ {
			for _, event := range a[from:to] {
				p.DoNewWithTreeSlow(event)
			}
			from += eventsPerIteration
			to += eventsPerIteration
		}
	})
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoFlatAllDeletedSimple(b *testing.B) {
	config := &Config{Fields: getFlatConfig()}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("old", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoOld(a[i])
		}
	})
	b.Run("tree_slow", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeSlow(a[i])
		}
	})
	b.Run("tree_fast", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTreeFast(a[i])
		}
	})
	b.Run("array_slow", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArraySlow(a[i])
		}
	})
	b.Run("array_fast", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArrayFast(a[i])
		}
	})
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoFlatAllSavedSimple(b *testing.B) {
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

func getFlatConfig() []string {
	result := make([]string, 10)
	for i := range result {
		result[i] = fmt.Sprintf("field%d", i+1)
	}

	return result
}

func getEventsAllFieldsSaved(b *testing.B, n int, fields []string) []*pipeline.Event {
	result := make([]*pipeline.Event, n)
	for i := 0; i < n; i++ {
		root, err := getEventAllFieldsSaved(fields)
		require.NoError(b, err)
		result[i] = &pipeline.Event{Root: root}
	}

	return result
}

func getEventAllFieldsSaved(fields []string) (*insaneJSON.Root, error) {
	root, err := insaneJSON.DecodeString("{}")
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		v := getRandValue(10)
		root.AddField(field).MutateToString(v)
	}

	return root, nil
}

func getEventsAllFieldsDeleted(b *testing.B, n int) []*pipeline.Event {
	result := make([]*pipeline.Event, n)
	for i := 0; i < n; i++ {
		root, err := getRandEvent(10)
		require.NoError(b, err)
		result[i] = &pipeline.Event{Root: root}
	}

	return result
}

func getRandEvent(fieldsCount int) (*insaneJSON.Root, error) {
	root, err := insaneJSON.DecodeString("{}")
	if err != nil {
		return nil, err
	}

	for i := 0; i < fieldsCount; i++ {
		k := getRandKey(8)
		v := getRandValue(10)
		root.AddField(k).MutateToString(v)
	}

	return root, nil
}

func getRandKey(length int) string {
	var b strings.Builder
	b.Grow(length)

	for i := 0; i < length; i++ {
		b.WriteByte(getRandByte('a', 'z'))
	}

	return b.String()
}

func getRandValue(length int) string {
	var b strings.Builder
	b.Grow(length)

	for i := 0; i < length; i++ {
		b.WriteByte(getRandByte(' ', '~'))
	}

	return b.String()
}

func getRandByte(from, to byte) byte {
	return from + byte(rand.Intn(int(to-from)))
}
