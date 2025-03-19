package keep_fields

import (
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

func TestKeepFieldsEmptyConfig(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{}}, nil)
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
	require.Equal(t, `{}`, outEvents[0], "wrong event")
	require.Equal(t, `{}`, outEvents[1], "wrong event")
	require.Equal(t, `{}`, outEvents[2], "wrong event")
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

func TestKeepNestedFieldsAllSaved(t *testing.T) {
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
	config := test.NewConfig(&Config{Fields: fields}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	testData := []struct {
		in  string
		out string
	}{
		{
			dataNested,
			dataNested,
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(testData))

	outEvents := make([]string, 0, len(testData))
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	for _, data := range testData {
		input.In(0, "test.log", test.NewOffset(0),
			[]byte(data.in),
		)
	}

	wg.Wait()
	p.Stop()

	require.Equal(t, len(testData), len(outEvents), "wrong out events count")
	for i, data := range testData {
		root, _ := insaneJSON.DecodeBytes([]byte(data.out))
		want := root.EncodeToString()
		require.Equal(t, want, outEvents[i], "wrong event")
	}
}

func TestKeepNestedFieldsNoSaved(t *testing.T) {
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
	config := test.NewConfig(&Config{Fields: fields}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	testData := []struct {
		in  string
		out string
	}{
		{
			dataNested,
			"{}",
		},
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(testData))

	outEvents := make([]string, 0, len(testData))
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	for _, data := range testData {
		input.In(0, "test.log", test.NewOffset(0),
			[]byte(data.in),
		)
	}

	wg.Wait()
	p.Stop()

	require.Equal(t, len(testData), len(outEvents), "wrong out events count")
	for i, data := range testData {
		root, _ := insaneJSON.DecodeBytes([]byte(data.out))
		want := root.EncodeToString()
		require.Equal(t, want, outEvents[i], "wrong event")
	}
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
