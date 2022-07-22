package throttle

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

type testConfig struct {
	t           *testing.T
	config      *Config
	eventsTotal int
	workTime    time.Duration
}

var formats = []string{
	`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
	`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_2"}`,
	`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_3"}`,
}

func (c *testConfig) runPipeline(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, c.config, pipeline.MatchModeAnd, nil, false))
	wgWithDeadline := atomic.NewInt32(int32(c.eventsTotal))

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wgWithDeadline.Dec()
	})

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	startTime := time.Now()
	for {
		index := rand.Int() % len(formats)
		// Format like RFC3339Nano, but nanoseconds are zero-padded, thus all times have equal length.
		json := fmt.Sprintf(formats[index], time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		if time.Since(startTime) > c.workTime {
			break
		}
	}

	p.Stop()
	tnow := time.Now()
	for {
		if wgWithDeadline.Load() <= 0 || time.Since(tnow) > 10*time.Second {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// if we have not generated the required amount messages for any bucket,
	// then we check the amount generated messages (outEvents) does not exceed the limit (eventsTotal)
	assert.True(c.t, c.eventsTotal <= len(outEvents), "wrong in events count")
}

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
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	tconf.runPipeline(t)
}

func TestSizeThrottle(t *testing.T) {
	buckets := 4
	sizeFraction := 100
	limitA := sizeFraction * 2
	limitB := sizeFraction * 3
	defaultLimit := sizeFraction * 20

	dateLen := len("2006-01-02T15:04:05.999999999Z")
	iterations := 5

	totalBuckets := iterations + 1
	eventsPerBucket := limitA/(len(formats[0])+dateLen-2) + limitB/(len(formats[1])+dateLen-2) + defaultLimit/(len(formats[2])+dateLen-2)
	eventsTotal := totalBuckets * eventsPerBucket

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
		LimitKind:      "size",
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	tconf.runPipeline(t)
}

func TestMixedThrottle(t *testing.T) {
	buckets := 2
	avgMessageSize := 90
	limitA := 2
	limitB := avgMessageSize * 3
	defaultLimit := 20

	iterations := 5

	totalBuckets := iterations + 1
	defaultLimitDelta := totalBuckets * defaultLimit
	eventsTotal := totalBuckets*(limitA+(limitB/avgMessageSize)) + defaultLimitDelta

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), LimitKind: "size", Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	workTime := config.BucketInterval_ * time.Duration(iterations)

	tconf := testConfig{t, config, eventsTotal, workTime}
	tconf.runPipeline(t)
}

func TestRedisThrottle(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	// set distributed redis limit
	require.NoError(t, s.Set("k8s_pod_pod_1_limit", "1"))

	defaultLimit := 20
	eventsTotal := 3

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:   1,
		BucketInterval: "2s",
		RedisBackendCfg: RedisKindConfig{
			SyncInterval: "100ms",
			Host:         s.Addr(),
			Password:     "",
		},
		LimiterBackend: "redis",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err = cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
	})

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	events := []string{
		`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_1"}`,
	}

	for i := 0; i < eventsTotal; i++ {
		json := fmt.Sprintf(events[i], time.Now().Format(time.RFC3339Nano))

		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))

		time.Sleep(300 * time.Millisecond)
	}

	p.Stop()

	assert.Greater(t, eventsTotal, len(outEvents), "wrong in events count")
}
