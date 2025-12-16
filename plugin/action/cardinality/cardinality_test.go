package cardinality

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestMapToStringSorted(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]string
		n        map[string]string
		expected string
	}{
		{
			name:     "empty maps",
			m:        map[string]string{},
			n:        map[string]string{},
			expected: "map[];map[]",
		},
		{
			name:     "simple maps",
			m:        map[string]string{"service": "test", "host": "localhost"},
			n:        map[string]string{"value": "1", "level": "3"},
			expected: "map[host:localhost service:test];map[level:3 value:1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapToStringSorted(tt.m, tt.n)
			if result != tt.expected {
				t.Errorf("mapToStringSorted() = %v, want %v", result, tt.expected)
			}

			// Verify the output is properly sorted
			if strings.HasPrefix(result, "map[") && strings.HasSuffix(result, "]") {
				content := result[4 : len(result)-1]
				if content != "" {
					pairs := strings.Split(content, " ")
					if !sort.StringsAreSorted(pairs) {
						t.Errorf("output pairs are not sorted: %v", pairs)
					}
				}
			}
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

	cacheKey := map[string]string{
		"host": "localhost",
	}
	cacheValue := map[string]string{
		"i": "0",
	}
	key := mapToKey(cacheKey)
	value := mapToStringSorted(cacheKey, cacheValue)
	cache.Set(value)

	keysCount := cache.CountPrefix(key)
	assert.Equal(t, 1, keysCount, "wrong in events count")
}
