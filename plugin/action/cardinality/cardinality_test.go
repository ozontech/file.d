package cardinality

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestParseFields(t *testing.T) {
	tests := []struct {
		name     string
		m        []cfg.FieldSelector
		v        []string
		expected string
	}{
		{
			name:     "empty maps",
			m:        []cfg.FieldSelector{},
			v:        []string{},
			expected: "[]",
		},
		{
			name:     "simple maps",
			m:        []cfg.FieldSelector{"service", "host"},
			v:        []string{"test", "localhost"},
			expected: "[service:test host:localhost]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedFields := parseFields(tt.m)
			for i := range tt.v {
				parsedFields.valsBuf[i] = tt.v[i]
			}

			var buf []byte
			result := parsedFields.appendTo(buf)

			assert.Equal(
				t,
				[]byte(tt.expected),
				result,
				"mapToStringSorted() = %v, want %v", string(result), string(tt.expected),
			)
		})
	}
}

func TestCardinalityLimitDiscard(t *testing.T) {
	limit := 10
	config := &Config{
		KeyFields: []cfg.FieldSelector{"info.host", "not_exists_fields"},
		Fields:    []cfg.FieldSelector{"value.i"},
		Limit:     limit,
		Action:    actionDiscard,
		TTL_:      1 * time.Hour,
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	inEventsCnt := 0
	inWg := sync.WaitGroup{}
	genEventsCnt := limit + 10
	inWg.Add(genEventsCnt)

	input.SetInFn(func() {
		defer inWg.Done()
		inEventsCnt++
	})

	outEventsCnt := 0
	output.SetOutFn(func(e *pipeline.Event) {
		outEventsCnt++
	})

	for i := 0; i < genEventsCnt; i++ {
		json := fmt.Sprintf(`{"info": {"host":"localhost"},"value":{"i":"%d"}}`, i)
		input.In(10, "test", test.NewOffset(0), []byte(json))
	}
	inWg.Wait()
	time.Sleep(100 * time.Millisecond)

	p.Stop()
	assert.Equal(t, inEventsCnt, genEventsCnt, "wrong in events count")
	assert.Equal(t, limit, outEventsCnt, "wrong out events count")
}

func TestCardinalityLimitRemoveFields(t *testing.T) {
	limit := 10
	config := &Config{
		KeyFields: []cfg.FieldSelector{"host"},
		Fields:    []cfg.FieldSelector{"i"},
		Limit:     limit,
		Action:    actionRemoveFields,
		TTL_:      1 * time.Hour,
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	inEventsCnt := 0
	inWg := sync.WaitGroup{}
	genEventsCnt := limit + 10
	inWg.Add(genEventsCnt)

	input.SetInFn(func() {
		defer inWg.Done()
		inEventsCnt++
	})

	outEventsCnt := 0
	wrongEventsCnt := 0
	outWg := sync.WaitGroup{}
	outWg.Add(genEventsCnt)
	output.SetOutFn(func(e *pipeline.Event) {
		defer outWg.Done()

		// check exists field
		value := pipeline.CloneString(e.Root.Dig(string(config.Fields[0])).AsString())
		keyValue := pipeline.CloneString(e.Root.Dig(string(config.KeyFields[0])).AsString())

		if value != "" {
			outEventsCnt++
		}

		if keyValue == "" {
			wrongEventsCnt++
		}
	})

	for i := 0; i < genEventsCnt; i++ {
		json := fmt.Sprintf(`{"host":"localhost","i":"%d"}`, i)
		input.In(10, "test", test.NewOffset(0), []byte(json))
	}
	inWg.Wait()
	outWg.Wait()

	p.Stop()
	assert.Equal(t, inEventsCnt, genEventsCnt, "wrong in events count")
	assert.Equal(t, 0, wrongEventsCnt)
	assert.Equal(t, limit, outEventsCnt, "wrong out events count")
}

func TestCardinalityLimitDiscardIfNoSetKeyFields(t *testing.T) {
	limit := 10
	config := &Config{
		KeyFields: []cfg.FieldSelector{},
		Fields:    []cfg.FieldSelector{"i"},
		Limit:     limit,
		Action:    actionDiscard,
		TTL_:      1 * time.Hour,
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	inEventsCnt := 0
	inWg := sync.WaitGroup{}
	genEventsCnt := limit + 10
	inWg.Add(genEventsCnt)

	input.SetInFn(func() {
		defer inWg.Done()
		inEventsCnt++
	})

	outEventsCnt := 0
	output.SetOutFn(func(e *pipeline.Event) {
		outEventsCnt++
	})

	for i := 0; i < genEventsCnt; i++ {
		json := fmt.Sprintf(`{"host":"localhost%d","i":"%d"}`, i, i)
		input.In(10, "test", test.NewOffset(0), []byte(json))
	}
	inWg.Wait()
	time.Sleep(100 * time.Millisecond)

	p.Stop()
	assert.Equal(t, inEventsCnt, genEventsCnt, "wrong in events count")
	assert.Equal(t, limit, outEventsCnt, "wrong out events count")
}

func TestSetAndCountPrefix(t *testing.T) {
	cache := NewCache(time.Minute)

	key := parseFields([]cfg.FieldSelector{"host"})
	key.valsBuf[0] = "localhost"

	value := parseFields([]cfg.FieldSelector{"i"})
	key.valsBuf[0] = "0"

	var buf []byte
	prefixKey := key.appendTo(buf)

	cache.Set(string(value.appendTo(prefixKey)))

	keysCount := cache.CountPrefix(string(prefixKey))
	assert.Equal(t, 1, keysCount, "wrong in events count")
}
