package modify

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{
		"_skip_empty":                      "true",
		"new_field":                        "new_value",
		"my_object.field.subfield":         "${existing_field}",
		"my_object.new_field.new_subfield": "new_subfield_value",
		"not_exists":                       "${not_existing_field}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, "new_value", e.Root.Dig("new_field").AsString(), "wrong event field")
		assert.Equal(t, "existing_value", e.Root.Dig("my_object", "field", "subfield").AsString(), "wrong event field")
		assert.Equal(t, "new_subfield_value", e.Root.Dig("my_object", "new_field", "new_subfield").AsString(), "wrong event field")
		assert.Nil(t, e.Root.Dig("not_exists"), "wrong event field")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"existing_field":"existing_value","my_object":{"field":{"subfield":"subfield_value"}}}`))

	wg.Wait()
	p.Stop()
}

func TestModifyRegex(t *testing.T) {
	testEvents := []struct {
		in           []byte
		fieldsValues map[string]string
	}{
		{
			in: []byte(`{"existing_field":"existing_value"}`),
			fieldsValues: map[string]string{
				"new_field":          "new_value",
				"substitution_field": "existing | value",
			},
		},
		{
			in: []byte(`{"other_field":"other_value"}`),
			fieldsValues: map[string]string{
				"new_field":          "new_value",
				"substitution_field": "",
			},
		},
		{
			in: []byte(`{"existing_field":"not_matched_re"}`),
			fieldsValues: map[string]string{
				"new_field":          "new_value",
				"substitution_field": "",
			},
		},
	}

	config := test.NewConfig(&Config{
		"new_field":          "new_value",
		"substitution_field": "${existing_field|re(\"(existing).*(value)\", -1, [1,2], \" | \", true)}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	outEvents := make([]string, 0, len(testEvents))
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})
	wg.Add(len(testEvents))

	for _, te := range testEvents {
		input.In(0, "test.log", test.Offset(0), te.in)
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, len(testEvents), len(outEvents), "wrong out events count")

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	for i := 0; i < len(testEvents); i++ {
		fvs := testEvents[i].fieldsValues
		_ = root.DecodeString(outEvents[i])
		for field := range fvs {
			wantVal := fvs[field]
			gotVal := root.Dig(field).AsString()
			assert.Equal(t, wantVal, gotVal, "wrong field value")
		}
	}
}

func TestModifyTrim(t *testing.T) {
	testEvents := []struct {
		in           []byte
		fieldsValues map[string]string
	}{
		{
			in: []byte(`{"existing_field":"existing_value"}`),
			fieldsValues: map[string]string{
				"new_field":          "new_value",
				"substitution_field": "value",
			},
		},
		{
			in: []byte(`{"other_field":"other_value"}`),
			fieldsValues: map[string]string{
				"new_field":          "new_value",
				"substitution_field": "",
			},
		},
	}

	config := test.NewConfig(&Config{
		"new_field":          "new_value",
		"substitution_field": "${existing_field|trim(\"left\",\"existing_\")}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	outEvents := make([]string, 0, len(testEvents))
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})
	wg.Add(len(testEvents))

	for _, te := range testEvents {
		input.In(0, "test.log", test.Offset(0), te.in)
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, len(testEvents), len(outEvents), "wrong out events count")

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	for i := 0; i < len(testEvents); i++ {
		fvs := testEvents[i].fieldsValues
		_ = root.DecodeString(outEvents[i])
		for field := range fvs {
			wantVal := fvs[field]
			gotVal := root.Dig(field).AsString()
			assert.Equal(t, wantVal, gotVal, "wrong field value")
		}
	}
}
