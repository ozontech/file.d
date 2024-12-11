package remove_fields

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveFields(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"field_1", "field_2"}}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"field_3":"value_3","a":"b"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"b"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"b":"c"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"field_3":"value_3","a":"b"}`, outEvents[2], "wrong event")
}

func TestRemoveNestedFields(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"a.b"}}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"a":"some"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"a":{"b":"deleted"}}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"a":{"b":{"c":["deleted"]},"d":"saved"}}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"some"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":{}}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":{"d":"saved"}}`, outEvents[2], "wrong event")
}

func TestDuplicatingFieldSelectors(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"a.b", "a.b"}}, nil)
	p := Plugin{}
	p.Start(config, nil)

	require.NotEmpty(t, p.fieldPaths)
	require.Equal(t, []string{"a", "b"}, p.fieldPaths[0])
}

func TestNestedFieldSelectors(t *testing.T) {
	config := test.NewConfig(&Config{Fields: []string{"a.b", "a.b.c", "a.d", "a"}}, nil)
	p := Plugin{}
	p.Start(config, nil)

	require.Equal(t, [][]string{{"a"}}, p.fieldPaths)
}
