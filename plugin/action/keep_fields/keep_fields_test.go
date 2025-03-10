package keep_fields

import (
	"fmt"
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

func BenchmarkDoFlatAllDeleted(b *testing.B) {
	config := &Config{Fields: getFlatConfig()}

	p := &Plugin{}

	p.Start(config, nil)

	const eventsPerIteration = 1

	b.Run("new_way_fast", func(b *testing.B) {
		n := b.N * eventsPerIteration
		a := getEventsAllFieldsDeleted(b, n)

		b.ResetTimer()

		from, to := 0, eventsPerIteration
		for i := 0; i < b.N; i++ {
			for _, event := range a[from:to] {
				p.DoNewWithArray(event)
			}
			from += eventsPerIteration
			to += eventsPerIteration
		}
	})
	b.Run("old_way", func(b *testing.B) {
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
	b.Run("new_way_tree_slow", func(b *testing.B) {
		n := b.N * eventsPerIteration
		a := getEventsAllFieldsDeleted(b, n)

		b.ResetTimer()

		from, to := 0, eventsPerIteration
		for i := 0; i < b.N; i++ {
			for _, event := range a[from:to] {
				p.DoNewWithTree(event)
			}
			from += eventsPerIteration
			to += eventsPerIteration
		}
	})
}

func BenchmarkDoFlatAllDeletedSimple(b *testing.B) {
	config := &Config{Fields: getFlatConfig()}

	p := &Plugin{}

	p.Start(config, nil)

	b.Run("new_way_fast", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
		}
	})
	b.Run("old_way", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoOld(a[i])
		}
	})
	b.Run("new_way_tree_slow", func(b *testing.B) {
		a := getEventsAllFieldsDeleted(b, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
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

const dataAllFieldsDeleted = `
{
  "menu0": "123123123",
  "menu1": "123123123",
  "menu2": "123123123",
  "menu3": "123123123",
  "menu4": "123123123",
  "menu5": "123123123",
  "menu6": "123123123",
  "menu7": "123123123",
  "menu8": "123123123",
  "menu9": "123123123"
}
`

func getEventsAllFieldsDeleted(b *testing.B, n int) []*pipeline.Event {
	result := make([]*pipeline.Event, n)
	for i := 0; i < n; i++ {
		root, err := insaneJSON.DecodeString(dataAllFieldsDeleted)
		require.NoError(b, err)
		result[i] = &pipeline.Event{Root: root}
	}

	return result
}
