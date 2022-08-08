package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func TestAppendEvent(t *testing.T) {
	metric.InitStats()
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
	}

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1})
	if err != nil {
		logger.Panic(err.Error())
	}

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithIndexOpType(t *testing.T) {
	stats.InitStats()
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "index",
	}

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1})
	if err != nil {
		logger.Panic(err.Error())
	}

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithCreateOpType(t *testing.T) {
	stats.InitStats()
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "create",
	}

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1})
	if err != nil {
		logger.Panic(err.Error())
	}

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"create":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestConfig(t *testing.T) {
	metric.InitStats()
	p := &Plugin{}
	config := &Config{
		IndexFormat: "test-%",
		Endpoints: []string{
			"http://endpoint_1:9000",
			"http://endpoint_2:9000/",
			"https://endpoint_3:9000",
			"https://endpoint_4:9000/",
		},
		BatchSize: "1",
	}

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1})
	if err != nil {
		logger.Panic(err.Error())
	}

	p.Start(config, test.NewEmptyOutputPluginParams())

	results := []string{
		"http://endpoint_1:9000/_bulk?_source=false",
		"http://endpoint_2:9000/_bulk?_source=false",
		"https://endpoint_3:9000/_bulk?_source=false",
		"https://endpoint_4:9000/_bulk?_source=false",
	}

	require.Len(t, p.endpoints, len(results))
	for i := range results {
		assert.Equal(t, results[i], p.endpoints[i].String())
	}
}
