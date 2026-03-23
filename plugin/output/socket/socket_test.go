package socket

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

func TestAppendEvent(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Address:   "test",
		BatchSize: "1",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	data := data{}
	event := &pipeline.Event{Root: root}

	data.outBuf, _ = event.Encode(data.outBuf)
	data.outBuf = append(data.outBuf, byte(0))

	expected := fmt.Sprintf("%s%c", `{"field_a":"AAAA","field_b":"BBBB"}`, byte(0))
	assert.Equal(t, expected, string(data.outBuf), "wrong request content")
}
