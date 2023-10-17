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

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

type testConfig struct {
	t           *testing.T
	config      *Config
	eventsTotal int
	iterations  int
}

var formats = []string{
	`{"time":"%s","k8s_ns":"ns_1","k8s_pod":"pod_1"}`,
	`{"time":"%s","k8s_ns":"ns_2","k8s_pod":"pod_2"}`,
	`{"time":"%s","k8s_ns":"not_matched","k8s_pod":"pod_3"}`,
}

func throttleMapsCleanup() {
	limitersMu.Lock()
	for k := range limiters {
		delete(limiters, k)
	}
	limitersMu.Unlock()
}

func (c *testConfig) runPipeline() {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, c.config, pipeline.MatchModeAnd, nil, false))

	limMap := limiters[p.Name]

	inEventsCnt := 0
	input.SetInFn(func() {
		inEventsCnt++
	})

	outEventsCnt := 0
	output.SetOutFn(func(e *pipeline.Event) {
		outEventsCnt++
	})

	sourceNames := []string{
		`source_1`,
		`source_2`,
		`source_3`,
	}

	// generating much more events per iteration than we need so that all buckets are filled
	genEventsCnt := 10
	if c.eventsTotal >= 0 {
		genEventsCnt = 10 * c.eventsTotal
	}

	bucketIntervalNS := c.config.BucketInterval_.Nanoseconds()
	startTime := time.Now()
	if startTime.UnixNano()%bucketIntervalNS > bucketIntervalNS/2 {
		startTime = startTime.Add(c.config.BucketInterval_ / 2)
	}
	for i := 0; i < c.iterations; i++ {
		curTime := startTime.Add(time.Duration(i) * c.config.BucketInterval_)
		limMap.setNowFn(func() time.Time {
			return curTime
		}, true)
		curTimeStr := curTime.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
		for j := 0; j < genEventsCnt; j++ {
			index := j % len(formats)
			// Format like RFC3339Nano, but nanoseconds are zero-padded, thus all times have equal length.
			json := fmt.Sprintf(formats[index], curTimeStr)
			input.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		}
		// just to make sure that events from the current iteration are processed in the plugin
		time.Sleep(10 * time.Millisecond)
	}

	p.Stop()

	// check that we passed expected amount of events
	if c.eventsTotal >= 0 {
		assert.Equal(c.t, c.eventsTotal, outEventsCnt, "wrong out events count")
	} else {
		assert.Equal(c.t, inEventsCnt, outEventsCnt, "wrong out events count")
	}
}

func TestThrottle(t *testing.T) {
	buckets := 2
	limitA := 2
	limitB := 3
	defaultLimit := 20

	iterations := 5

	defaultLimitDelta := iterations * defaultLimit
	eventsTotal := iterations*(limitA+limitB) + defaultLimitDelta

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
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	tconf := testConfig{t, config, eventsTotal, iterations}
	tconf.runPipeline()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestThrottleNoLimit(t *testing.T) {
	buckets := 2
	limitA := -2
	limitB := -3
	defaultLimit := -20

	iterations := 5

	eventsTotal := -1

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
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	tconf := testConfig{t, config, eventsTotal, iterations}
	tconf.runPipeline()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestSizeThrottle(t *testing.T) {
	buckets := 4
	sizeFraction := 100
	limitA := sizeFraction * 2
	limitB := sizeFraction * 3
	defaultLimit := sizeFraction * 20

	dateLen := len("2006-01-02T15:04:05.999999999Z")
	iterations := 5

	eventsPerBucket := limitA/(len(formats[0])+dateLen-2) + limitB/(len(formats[1])+dateLen-2) + defaultLimit/(len(formats[2])+dateLen-2)
	eventsTotal := iterations * eventsPerBucket

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
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	tconf := testConfig{t, config, eventsTotal, iterations}
	tconf.runPipeline()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestMixedThrottle(t *testing.T) {
	buckets := 2
	avgMessageSize := 90
	limitA := 2
	limitB := avgMessageSize * 3
	defaultLimit := 20

	dateLen := len("2006-01-02T15:04:05.999999999Z")
	iterations := 5

	defaultLimitDelta := iterations * defaultLimit
	eventsTotal := iterations*(limitA+(limitB/(len(formats[1])+dateLen-2))) + defaultLimitDelta

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
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	tconf := testConfig{t, config, eventsTotal, iterations}
	tconf.runPipeline()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
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
			Endpoint:     s.Addr(),
			Password:     "",
			SyncInterval: "100ms",
			WorkerCount:  2,
		},
		LimiterBackend: "redis",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}

	cfg.SetDefaultValues(config)
	err = cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	outEvents := 0
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents++
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

	assert.Greater(t, eventsTotal, outEvents, "wrong in events count")
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestRedisThrottleMultiPipes(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	defaultLimit := 20

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:   1,
		BucketInterval: "2m",
		RedisBackendCfg: RedisBackendConfig{
			Endpoint:     s.Addr(),
			Password:     "",
			SyncInterval: "10ms",
			WorkerCount:  2,
		},
		LimiterBackend: "redis",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	cfg.SetDefaultValues(config)
	err = cfg.Parse(config, nil)
	require.NoError(t, err)

	muFirstPipe := sync.Mutex{}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false), "name")
	outEvents := 0
	output.SetOutFn(func(e *pipeline.Event) {
		muFirstPipe.Lock()
		defer muFirstPipe.Unlock()
		outEvents++
	})

	muSecPipe := sync.Mutex{}
	pSec, inputSec, outputSec := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false), "name")
	outEventsSec := 0
	outputSec.SetOutFn(func(e *pipeline.Event) {
		muSecPipe.Lock()
		defer muSecPipe.Unlock()
		outEventsSec++
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
	assert.Greater(t, len(firstPipeEvents), outEvents, "wrong in events count")

	for i := 0; i < len(secondPipeEvents); i++ {
		json := fmt.Sprintf(secondPipeEvents[i], time.Now().Format(time.RFC3339Nano))
		inputSec.In(10, sourceNames[rand.Int()%len(sourceNames)], 0, []byte(json))
		// timeout required due shifting time call to redis
		time.Sleep(100 * time.Millisecond)
	}

	muSecPipe.Lock()
	defer muSecPipe.Unlock()

	// limit is 10 while events count 4, all passed
	assert.Equal(t, len(secondPipeEvents), outEventsSec, "wrong in events count")
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
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
			Endpoint:          s.Addr(),
			Password:          "",
			LimiterKeyField:   "throttle_key",
			LimiterKeyField_:  []string{"throttle_key"},
			LimiterValueField: "count_limit",
			SyncInterval:      "100ms",
			WorkerCount:       2,
		},
		LimiterBackend: "redis",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
	}
	cfg.SetDefaultValues(config)
	err = cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	p, input, output := test.NewPipelineMock(
		test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false),
		"name",
	)
	outEvents := 0
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents++
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

	assert.Greater(t, eventsTotal, outEvents, "wrong in events count")
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestThrottleLimiterExpiration(t *testing.T) {
	defaultLimit := 3
	eventsTotal := 3
	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(defaultLimit), LimitKind: "count"},
		},
		BucketsCount:      1,
		BucketInterval:    "100ms",
		ThrottleField:     "k8s_pod",
		TimeField:         "",
		DefaultLimit:      int64(defaultLimit),
		LimiterExpiration: "300ms",
	}
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	p, input, _ := test.NewPipelineMock(
		test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false),
		"name",
	)

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

		time.Sleep(10 * time.Millisecond)
	}
	limitersMu.RLock()
	lm, has := limiters[p.Name]
	limitersMu.RUnlock()
	assert.True(t, has, "key must exist in the map")
	assert.NotNil(t, lm, "the map object must be non-nil")
	lm.mu.RLock()
	lim, has := lm.lims["a:pod_1"]
	lm.mu.RUnlock()
	assert.True(t, has, "key must exist in the map")
	assert.NotNil(t, lim, "the map object must be non-nil")
	time.Sleep(time.Second)
	lm.mu.RLock()
	_, has = lm.lims["a:pod_1"]
	lm.mu.RUnlock()
	assert.False(t, has, "key must not exist in the map")

	p.Stop()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}

func TestThrottleRedisFallbackToInMemory(t *testing.T) {
	buckets := 2
	limitA := 2
	limitB := 3
	defaultLimit := 20

	iterations := 5

	defaultLimitDelta := iterations * defaultLimit
	eventsTotal := iterations*(limitA+limitB) + defaultLimitDelta

	config := &Config{
		Rules: []RuleConfig{
			{Limit: int64(limitA), Conditions: map[string]string{"k8s_ns": "ns_1"}},
			{Limit: int64(limitB), Conditions: map[string]string{"k8s_ns": "ns_2"}},
		},
		RedisBackendCfg: RedisBackendConfig{
			Endpoint:     "invalid_redis",
			Password:     "",
			SyncInterval: "100ms",
			WorkerCount:  2,
		},
		BucketsCount:   buckets,
		BucketInterval: "100ms",
		ThrottleField:  "k8s_pod",
		TimeField:      "",
		DefaultLimit:   int64(defaultLimit),
		LimiterBackend: "redis",
	}
	cfg.SetDefaultValues(config)
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panic(err.Error())
	}

	tconf := testConfig{t, config, eventsTotal, iterations}
	tconf.runPipeline()
	t.Cleanup(func() {
		throttleMapsCleanup()
	})
}
