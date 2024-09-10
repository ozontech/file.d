package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func TestAppendEvent(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithIndexOpType(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "index",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithCreateOpType(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "create",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"create":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestPrepareEndpoints(t *testing.T) {
	in := []string{
		"http://endpoint_1:9000",
		"http://endpoint_2:9000/",
		"https://endpoint_3:9000",
		"https://endpoint_4:9000/",
	}
	want := []string{
		"http://endpoint_1:9000/_bulk?_source=false",
		"http://endpoint_2:9000/_bulk?_source=false",
		"https://endpoint_3:9000/_bulk?_source=false",
		"https://endpoint_4:9000/_bulk?_source=false",
	}

	got := prepareEndpoints(in)

	require.Len(t, got, len(want))
	for i := range got {
		assert.Equal(t, want[i], got[i])
	}
}
