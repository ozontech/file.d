package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metrics"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func TestAppendEvent(t *testing.T) {
	metrics.InitStats()
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

func TestConfig(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		IndexFormat: "test-%",
		Endpoints:   []string{"http://endpoint_1:9000", "http://endpoint_2:9000/"},
		BatchSize:   "1",
	}

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1})
	if err != nil {
		logger.Panic(err.Error())
	}

	p.Start(config, test.NewEmptyOutputPluginParams())

	assert.Equal(t, "http://endpoint_1:9000/_bulk?_source=false", p.config.Endpoints[0], "wrong endpoint")
	assert.Equal(t, "http://endpoint_2:9000/_bulk?_source=false", p.config.Endpoints[1], "wrong endpoint")
}
