package rename

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRename(t *testing.T) {
	t.Parallel()

	config := &Config{
		"field_1", "renamed_field_1",
		"field_2", "renamed_field_2",
		"field_4.field_5", "renamed_field_5",
		"field_5", "renamed_field_2",
		"k8s_node_label_topology\\.kubernetes\\.io/zone", "renamed_field.escaped",
		"override", "false",
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(5)

	outEvents := make([]string, 0, 5)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"field_1":"value_1"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"field_2":"value_2"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"field_3":"value_3"}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"field_4":{"field_5":"value_5"}}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"k8s_node_label_topology.kubernetes.io/zone":"value_6"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 5, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"renamed_field_1":"value_1"}`, outEvents[0], "wrong event json")
	assert.Equal(t, `{"renamed_field_2":"value_2"}`, outEvents[1], "wrong event json")
	assert.Equal(t, `{"field_3":"value_3"}`, outEvents[2], "wrong event json")
	assert.Equal(t, `{"field_4":{},"renamed_field_5":"value_5"}`, outEvents[3], "wrong event json")
	assert.Equal(t, `{"renamed_field.escaped":"value_6"}`, outEvents[4], "wrong event json")
}

func TestRenamingSequence(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	config := &Config{
		"key1", "key2",
		"key2", "key3",
		"key3", "key4",
		"key4", "key5",
		"key5", "key6",
		"key6", "key7",
		"key7", "key8",
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	wg := sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		r.Equal(`{"key8":"value_1"}`, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"key1":"value_1"}`))

	wg.Wait()
	p.Stop()
}

func TestUnescapeMap(t *testing.T) {
	t.Parallel()

	conf := Config{
		"___key1", "val1",
		"__key2", "val2",
		"_key3", "val3",
		"key4", "val4",
	}
	actual := unescapeMap(conf)

	expected := Config{
		"__key1", "val1",
		"_key2", "val2",
		"key3", "val3",
		"key4", "val4",
	}
	require.Equal(t, expected, actual)
}
