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
	b.Run("tree", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsNoFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
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
	b.Run("tree", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsHalfFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
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
	b.Run("tree", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsAllFieldsSaved(b, b.N, fields)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
		}
	})
}

const dataNested = `
{
	"level11": "qwerty",
	"level12": "some",
	"level13": 123,
	"level14": true,
	"some11": {"k1":"v1","k2":"v2","k3":"v3"},
	"some12": {"k1":"v1","k2":"v2","k3":"v3"},
	"some13": {"k1":"v1","k2":"v2","k3":"v3"},
	"some14": {"k1":"v1","k2":"v2","k3":"v3"},
	"qwe31": {"k1":"v1","k2":"v2","k3":{"k1":"v1","k2":"v2","k3":"v3"}},
	"qwe32": {"k1":"v1","k2":"v2","k3":{"k1":"v1","k2":"v2","k3":"v3"}}
}
`

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoNestedNoFieldsSaved(b *testing.B) {
	fields := []string{
		// not found
		"some.qwe.aaa",
		"some.qwe.bbb",
		"some.qwe.ccc",
		"qwe",

		// almost found
		"level11.empty",
		"level12.empty",
		"level13.empty",
		"level14.empty",
		"qwe31.k1.empty",
		"qwe31.k2.empty",
		"qwe31.k3.k1.empty",
		"qwe31.k3.k2.empty",
		"qwe31.k3.k3.empty",

		// not found
		"field1",
		"field2",
		"field3",
		"abcd.aaa",
		"abcd.bbb",
		"abcd.ccc",

		// almost found
		"some12.k1.empty",
		"some12.k2.empty",
		"some12.k3.empty",
		"some12.k4",
		"some14.k1.empty",
		"some14.k2.empty",
		"some14.k3.empty",
		"some14.k4",
	}
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("tree", func(b *testing.B) {
		a := getEventsBySrc(b, dataNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsBySrc(b, dataNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
		}
	})
}

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoNestedAllFieldsSaved(b *testing.B) {
	fields := []string{
		// found
		"level11",
		"level12",
		"level13",
		"level14",

		// not found
		"level15",
		"level16",
		"level17",
		"level18",

		// found
		"some11.k1",
		"some11.k2",
		"some11.k3",
		"some12",
		"some13.k1",
		"some13.k2",
		"some13.k3",
		"some14.k1",
		"some14.k2",
		"some14.k3",

		// not found
		"some14.k4",
		"some14.k5",
		"some14.k6",

		// found
		"qwe31.k1",
		"qwe31.k2",
		"qwe31.k3.k1",
		"qwe31.k3.k2",
		"qwe31.k3.k3",
		"qwe32.k1",
		"qwe32.k2",
		"qwe32.k3",
	}
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("tree", func(b *testing.B) {
		a := getEventsBySrc(b, dataNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsBySrc(b, dataNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
		}
	})
}

const dataDeepNested = `
{"aaaaaaaa":
{"bbbbbbbb":
{"cccccccc":
{"dddddddd":
{"eeeeeeee":
{"ffffffff":
{"gggggggg":
{"hhhhhhhh":
{
	"f1":2,
	"f2":2,
	"f3":2,
	"f4":2,
	"f5":2,
	"f6":2,
	"f7":2,
	"f8":2
}
}}}}}}}}
`

// NOTE: run it with flags: -benchtime 10ms -count 5
func BenchmarkDoDeepNestedAllFieldsSaved(b *testing.B) {
	fields := []string{
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f1",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f2",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f3",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f4",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f5",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f6",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f7",
		"aaaaaaaa.bbbbbbbb.cccccccc.dddddddd.eeeeeeee.ffffffff.gggggggg.hhhhhhhh.f8",
	}
	config := &Config{Fields: fields}

	p := &Plugin{}

	p.Start(config, nil)

	debug.SetGCPercent(-1)

	b.Run("tree", func(b *testing.B) {
		a := getEventsBySrc(b, dataDeepNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithTree(a[i])
		}
	})
	b.Run("array", func(b *testing.B) {
		a := getEventsBySrc(b, dataDeepNested, b.N)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.DoNewWithArray(a[i])
		}
	})
}

func TestParseNestedFields(t *testing.T) {
	type TestCase struct {
		in  []string
		out [][]string
	}

	for _, tt := range []TestCase{
		{
			// simple
			in:  []string{"a", "b", "c"},
			out: [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			// empty
			in:  []string{"", "a", "", "", "b", ""},
			out: [][]string{{"a"}, {"b"}},
		},
		{
			// simple duplicates
			in:  []string{"a", "b", "b", "a", "c", "a", "c", "b"},
			out: [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			// nested duplicates
			in:  []string{"a.b", "some.qwe", "some.qwe", "a.b"},
			out: [][]string{{"a", "b"}, {"some", "qwe"}},
		},
		{
			// nested duplicates and nested field
			in:  []string{"a.b.c", "a.b", "a.b"},
			out: [][]string{{"a", "b"}},
		},
		{
			// field name prefix
			in:  []string{"prefix", "prefix_some"},
			out: [][]string{{"prefix"}, {"prefix_some"}},
		},
		{
			// nested field name prefix
			in:  []string{"qwe12.some.f1", "qwe"},
			out: [][]string{{"qwe"}, {"qwe12", "some", "f1"}},
		},
		{
			// nested field name prefix
			in:  []string{"qwe.some12.f1", "qwe.some"},
			out: [][]string{{"qwe", "some"}, {"qwe", "some12", "f1"}},
		},
		{
			// many nested fields
			in:  []string{"a.b", "a.b.c", "a.d", "a"},
			out: [][]string{{"a"}},
		},
		{
			in:  []string{"a.b.f1", "a.b.f2"},
			out: [][]string{{"a", "b", "f1"}, {"a", "b", "f2"}},
		},
		{
			in:  []string{"a.b.f1", "a.b.f2", "a.b", "a.c"},
			out: [][]string{{"a", "b"}, {"a", "c"}},
		},
		{
			in:  []string{"a.b.f1", "a.b.f2", "a.b", "a.c", "a"},
			out: [][]string{{"a"}},
		},
	} {
		require.Equal(t, tt.out, parseNestedFields(tt.in))
	}
}
