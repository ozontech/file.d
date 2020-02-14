package throttle

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestThrottle(t *testing.T) {
	buckets := 2
	limitA := 2
	limitB := 3
	defaultLimit := 20

	iterations := 5

	totalBuckets := iterations + 1
	defaultLimitDelta := totalBuckets * defaultLimit
	eventsTotal := totalBuckets*(limitA+limitB) + defaultLimitDelta

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "time",
		DefaultLimit:   int64(defaultLimit),
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))
	wg := &sync.WaitGroup{}
	wg.Add(eventsTotal)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
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

		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		if time.Now().Sub(startTime) > workTime {
			break
		}
	}

	p.Stop()
	wg.Wait()

	assert.Equal(t, eventsTotal, len(outEvents), "wrong in events count")
}
