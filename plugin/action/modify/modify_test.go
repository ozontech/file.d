package modify

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{
		"new_field":                        "new_value",
		"my_object.field.subfield":         "${existing_field}",
		"my_object.new_field.new_subfield": "new_subfield_value",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, "new_value", e.Root.Dig("new_field").AsString(), "wrong event field")
		assert.Equal(t, "existing_value", e.Root.Dig("my_object", "field", "subfield").AsString(), "wrong event field")
		assert.Equal(t, "new_subfield_value", e.Root.Dig("my_object", "new_field", "new_subfield").AsString(), "wrong event field")
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"existing_field":"existing_value","my_object":{"field":{"subfield":"subfield_value"}}}`))

	wg.Wait()
	p.Stop()
}

func TestModifyRegex(t *testing.T) {
	testEvents := []struct {
		in           []byte
		fieldsValues map[string]string
	}{
		{
			[]byte(`{"existing_field":"existing_value"}`),
			map[string]string{
				"new_field":          "new_value",
				"substitution_field": "existing | value",
			},
		},
		{
			[]byte(`{"other_field":"other_value"}`),
			map[string]string{
				"new_field":          "new_value",
				"substitution_field": "",
			},
		},
	}

	config := test.NewConfig(&Config{
		"new_field":          "new_value",
		"substitution_field": "${existing_field|re(\"(existing).*(value)\", -1, [1,2], \" | \")}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	outEvents := struct {
		mu     sync.Mutex
		events []*pipeline.Event
	}{
		events: make([]*pipeline.Event, 0),
	}
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents.mu.Lock()
		outEvents.events = append(outEvents.events, e)
		outEvents.mu.Unlock()
		wg.Done()
	})
	wg.Add(len(testEvents))

	for _, te := range testEvents {
		input.In(0, "test.log", 0, te.in)
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, len(testEvents), len(outEvents.events), "wrong out events count")
	for i := 0; i < len(testEvents); i++ {
		fvs := testEvents[i].fieldsValues
		for field := range fvs {
			wantVal := fvs[field]
			outEvents.mu.Lock()
			gotVal := outEvents.events[i].Root.Dig(field).AsString()
			outEvents.mu.Unlock()
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
			[]byte(`{"existing_field":"existing_value"}`),
			map[string]string{
				"new_field":          "new_value",
				"substitution_field": "value",
			},
		},
		{
			[]byte(`{"other_field":"other_value"}`),
			map[string]string{
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

	outEvents := struct {
		mu     sync.Mutex
		events []*pipeline.Event
	}{
		events: make([]*pipeline.Event, 0),
	}

	output.SetOutFn(func(e *pipeline.Event) {
		outEvents.mu.Lock()
		outEvents.events = append(outEvents.events, e)
		outEvents.mu.Unlock()
		wg.Done()
	})
	wg.Add(len(testEvents))

	for _, te := range testEvents {
		input.In(0, "test.log", 0, te.in)
	}

	wg.Wait()
	p.Stop()

	assert.Equal(t, len(testEvents), len(outEvents.events), "wrong out events count")
	for i := 0; i < len(testEvents); i++ {
		fvs := testEvents[i].fieldsValues
		for field := range fvs {
			wantVal := fvs[field]
			outEvents.mu.Lock()
			gotVal := outEvents.events[i].Root.Dig(field).AsString()
			outEvents.mu.Unlock()
			assert.Equal(t, wantVal, gotVal, "wrong field value")
		}
	}
}
