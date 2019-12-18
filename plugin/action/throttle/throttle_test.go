package throttle

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

func startPipeline(interval time.Duration, buckets, limitA, limitB, defaultLimit int) (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	p := pipeline.NewTestPipeLine(false)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: fake.Config{}}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		Buckets:       buckets,
		Interval:      pipeline.Duration{Duration: interval},
		ThrottleField: "k8s_pod",
		TimeField:     "time",
		DefaultLimit:  int64(defaultLimit),
	}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestThrottle(t *testing.T) {
	interval := time.Millisecond * 100
	buckets := 2
	limitA := 2
	limitB := 3
	defaultLimit := 20

	iterations := 5
	workTime := interval * time.Duration(iterations)

	p, input, output := startPipeline(interval, buckets, limitA, limitB, defaultLimit)

	events := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		events = append(events, e)
	})

	formats := []string{
		`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_2"}`,
		`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_3"}`,
	}

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	startTime := time.Now()
	for {
		index := rand.Int() % len(formats)
		json := fmt.Sprintf(formats[index], time.Now().Format(time.RFC3339Nano))

		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, 0, []byte(json))
		if time.Now().Sub(startTime) > workTime {
			break
		}
	}

	p.Stop()

	totalBuckets := iterations + 1
	defaultLimitDelta := totalBuckets * defaultLimit
	assert.Equal(t, totalBuckets*(limitA+limitB)+defaultLimitDelta, len(events), "wrong accepted events count")
}
