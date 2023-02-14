package throttle

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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

func (c *testConfig) runPipeline() {
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
	tconf.runPipeline()
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
	tconf.runPipeline()
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
	tconf.runPipeline()
}

func TestRedisThrottle(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	// set distributed redis limit
	require.NoError(t, s.Set("test_pipeline_k8s_pod_pod_1_limit", "1"))

	defaultLimit := 3
	eventsTotal := 3

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:   1,
		BucketInterval: "2s",
		RedisBackendCfg: RedisBackendConfig{
			SyncInterval: "100ms",
			Endpoint:     s.Addr(),
			Password:     "",
			WorkerCount:  2,
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

func TestRedisThrottleMultiPipes(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	defaultLimit := 20

	config := Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:   1,
		BucketInterval: "2m",
		RedisBackendCfg: RedisBackendConfig{
			SyncInterval: "10ms",
			Endpoint:     s.Addr(),
			Password:     "",
			WorkerCount:  2,
		},
		LimiterBackend: "redis",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	err = cfg.Parse(&config, nil)
	require.NoError(t, err)

	muFirstPipe := sync.Mutex{}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, &config, pipeline.MatchModeAnd, nil, false), "name")
	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		muFirstPipe.Lock()
		defer muFirstPipe.Unlock()
		outEvents = append(outEvents, e)
	})

	muSecPipe := sync.Mutex{}
	pSec, inputSec, outputSec := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, &config, pipeline.MatchModeAnd, nil, false), "name")
	outEventsSec := make([]*pipeline.Event, 0)
	outputSec.SetOutFn(func(e *pipeline.Event) {
		muSecPipe.Lock()
		defer muSecPipe.Unlock()
		outEventsSec = append(outEventsSec, e)
	})

	// set distributed redis limit
	require.NoError(t, s.Set(fmt.Sprintf("%s_%s", p.Name, "k8s_pod_pod_1_limit"), "1"))
	require.NoError(t, s.Set(fmt.Sprintf("%s_%s", pSec.Name, "k8s_pod_pod_1_limit"), "5"))

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	firstPipeEvents := []string{
		`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_1"}`,
	}
	secondPipeEvents := []string{
		`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_3","k8s_pod":"pod_1"}`,
		`{"time":"%s","k8s_ns":"ns_4","k8s_pod":"pod_1"}`,
	}
	for i := 0; i < len(firstPipeEvents); i++ {
		json := fmt.Sprintf(firstPipeEvents[i], time.Now().Format(time.RFC3339Nano))
		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		// timeout required due shifting time call to redis
		time.Sleep(100 * time.Millisecond)
	}
	// limit is 1 while events count is 3
	assert.Greater(t, len(firstPipeEvents), len(outEvents), "wrong in events count")

	for i := 0; i < len(secondPipeEvents); i++ {
		json := fmt.Sprintf(secondPipeEvents[i], time.Now().Format(time.RFC3339Nano))
		inputSec.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		// timeout required due shifting time call to redis
		time.Sleep(100 * time.Millisecond)
	}
	// limit is 10 while events count 4, all passed
	assert.Equal(t, len(secondPipeEvents), len(outEventsSec), "wrong in events count")
}

func TestRedisThrottleWithCustomLimitData(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	// set distributed redis limit
	require.NoError(t, s.Set("custom_limit_key", `{"count_limit":"1"}`))

	defaultLimit := 3
	eventsTotal := 3
	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:   1,
		BucketInterval: "2s",
		RedisBackendCfg: RedisBackendConfig{
			SyncInterval:      "100ms",
			Endpoint:          s.Addr(),
			Password:          "",
			WorkerCount:       2,
			LimiterKeyField:   "throttle_key",
			LimiterKeyField_:  []string{"throttle_key"},
			LimiterValueField: "count_limit",
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

	p, input, output := test.NewPipelineMock(
		test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false),
		"name",
	)
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
		`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1","throttle_key":"custom_limit_key"}`,
		`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_1","throttle_key":"custom_limit_key"}`,
		`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_1","throttle_key":"custom_limit_key"}`,
	}

	nowTs := time.Now().Format(time.RFC3339Nano)
	for i := 0; i < eventsTotal; i++ {
		json := fmt.Sprintf(events[i], nowTs)

		input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))

		time.Sleep(300 * time.Millisecond)
	}

	p.Stop()

	assert.Greater(t, eventsTotal, len(outEvents), "wrong in events count")
}
