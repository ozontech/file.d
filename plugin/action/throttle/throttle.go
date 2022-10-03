package throttle

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

var (
	defaultThrottleKey = "default"

	// limiters should be shared across pipeline, so let's have a map by namespace and limiter name
	limiters                           = map[string]map[string]limiter{} // todo: cleanup this map?
	limitersMu                         = &sync.RWMutex{}
	redisLimiterSynchronizationStarted = map[string]struct{}{}
)

const (
	redisBackend    = "redis"
	inMemoryBackend = "memory"
)

// interface with only necessary functions of the original redis.Client
type redisClient interface {
	IncrBy(key string, value int64) *redis.IntCmd
	Expire(key string, expiration time.Duration) *redis.BoolCmd
	SetNX(key string, value any, expiration time.Duration) *redis.BoolCmd
	Get(key string) *redis.StringCmd
	Ping() *redis.StatusCmd
}
type limiter interface {
	isAllowed(event *pipeline.Event, ts time.Time) bool
	sync()
}

/*{ introduction
It discards the events if pipeline throughput gets higher than a configured threshold.
}*/

type Plugin struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zap.SugaredLogger
	config      *Config
	pipeline    string
	format      string
	redisClient redisClient

	limiterBuf []byte
	rules      []*rule
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which will be used as a key for throttling.
	// > It means that throttling will work separately for events with different keys.
	// > If not set, it's assumed that all events have the same key.
	ThrottleField  cfg.FieldSelector `json:"throttle_field" default:"" parse:"selector"` // *
	ThrottleField_ []string

	// > @3@4@5@6
	// >
	// > The event field which defines the time when event was fired.
	// > It is used to detect the event throughput in a particular time range.
	// > If not set, the current time will be taken.
	TimeField  cfg.FieldSelector `json:"time_field" default:"time" parse:"selector"` // *
	TimeField_ []string

	// > @3@4@5@6
	// >
	// > It defines how to parse the time field format.
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"` // *

	// > @3@4@5@6
	// >
	// > The default events limit that plugin allows per `interval`.
	DefaultLimit int64 `json:"default_limit" default:"5000"` // *

	// > @3@4@5@6
	// >
	// > It defines subject of limiting: number of messages or total size of the messages.
	LimitKind string `json:"limit_kind" default:"count" options:"count|size"` // *

	// > @3@4@5@6
	// >
	// > Defines kind of backend.
	LimiterBackend string `json:"limiter_backend" default:"memory" options:"memory|redis"` // *

	// > @3@4@5@6
	// >
	// > It contains redis settings
	RedisBackendCfg RedisBackendConfig `json:"redis_backend_config" child:"true"` // *

	// > @3@4@5@6
	// >
	// > How much time buckets to hold in the memory. E.g. if `buckets_count` is `60` and `interval` is `5m`,
	// > then `5 hours` will be covered. Events with time later than `now() - 5h` will be dropped even if threshold isn't exceeded.
	BucketsCount int `json:"buckets_count" default:"60"` // *

	// > @3@4@5@6
	// >
	// > Time interval to check event throughput.
	BucketInterval  cfg.Duration `json:"bucket_interval" parse:"duration" default:"1m"` // *
	BucketInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > Rules to override the `default_limit` for different group of event. It's a list of objects.
	// > Each object has the `limit` and `conditions` fields.
	// > * `limit` – the value which will override the `default_limit`, if `conditions` are met.
	// > * `limit_kind` – the type of a limit: `count` - number of messages, `size` - total size from all messages
	// > * `conditions` – the map of `event field name => event field value`. The conditions are checked using `AND` operator.
	Rules []RuleConfig `json:"rules" default:"" slice:"true"` // *
}

type RedisBackendConfig struct {
	// > @3@4@5@6
	// >
	// Аddress of redis server. Format: HOST:PORT.
	Endpoint string `json:"endpoint"` // *

	// > @3@4@5@6
	// >
	// > Password to redis server.
	Password string `json:"password"` // *

	// > @3@4@5@6
	// >
	// > Defines sync interval between global and local limiters.
	SyncInterval  cfg.Duration `json:"sync_interval" parse:"duration" default:"5s"` // *
	SyncInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > Defines num of parallel workers that will sync limits.
	WorkerCount int `json:"worker_count" default:"32"` // *

	// > @3@4@5@6
	// >
	// > Defines redis timeout.
	Timeout  cfg.Duration `json:"timeout" parse:"duration" default:"1s"` // *
	Timeout_ time.Duration
}

type RuleConfig struct {
	Limit      int64             `json:"limit"`
	LimitKind  string            `json:"limit_kind" default:"count" options:"count|size"`
	Conditions map[string]string `json:"conditions"`
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "throttle",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) syncWorker(ctx context.Context, jobCh <-chan limiter, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobCh:
			job.sync()
			wg.Done()
		}
	}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger
	p.pipeline = params.PipelineName
	p.limiterBuf = make([]byte, 0)
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel

	limitersMu.Lock()
	limiters[p.pipeline] = map[string]limiter{}
	limitersMu.Unlock()

	format, err := pipeline.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.format
	}
	p.format = format

	if p.config.LimiterBackend == redisBackend {
		if p.config.RedisBackendCfg.WorkerCount < 1 {
			p.logger.Fatalf("workers_count must be > 0, passed: %d", p.config.RedisBackendCfg.WorkerCount)
		}

		p.redisClient = redis.NewClient(
			&redis.Options{
				Network:      "tcp",
				Addr:         p.config.RedisBackendCfg.Endpoint,
				Password:     p.config.RedisBackendCfg.Password,
				ReadTimeout:  p.config.RedisBackendCfg.Timeout_,
				WriteTimeout: p.config.RedisBackendCfg.Timeout_,
			})

		if pingResp := p.redisClient.Ping(); pingResp.Err() != nil {
			p.logger.Fatalf("can't ping redis: %s", pingResp.Err())
		}

		limitersMu.Lock()
		// run sync only once per pipeline
		if _, ok := redisLimiterSynchronizationStarted[p.pipeline]; !ok {
			redisLimiterSynchronizationStarted[p.pipeline] = struct{}{}

			go p.runSync(ctx)
		}
		limitersMu.Unlock()
	}

	for i, r := range p.config.Rules {
		p.rules = append(p.rules, NewRule(r.Conditions, complexLimit{r.Limit, r.LimitKind}, i))
	}

	p.rules = append(p.rules, NewRule(map[string]string{}, complexLimit{p.config.DefaultLimit, p.config.LimitKind}, len(p.config.Rules)))
}

// runSync runs synchronization with redis.
func (p *Plugin) runSync(ctx context.Context) {
	ticker := time.NewTicker(p.config.RedisBackendCfg.SyncInterval_)
	wg := sync.WaitGroup{}

	jobs := make(chan limiter, p.config.RedisBackendCfg.WorkerCount)
	for i := 0; i < p.config.RedisBackendCfg.WorkerCount; i++ {
		go p.syncWorker(ctx, jobs, &wg)
	}

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			return
		case <-ticker.C:
			limitersMu.RLock()

			for _, lim := range limiters[p.pipeline] {
				wg.Add(1)
				jobs <- lim
			}
			wg.Wait()
			limitersMu.RUnlock()
		}
	}
}

func (p *Plugin) getNewLimiter(throttleField, limiterKey string, rule *rule) limiter {
	switch p.config.LimiterBackend {
	case redisBackend:
		return NewRedisLimiter(
			p.ctx,
			p.redisClient,
			p.pipeline,
			throttleField,
			limiterKey,
			p.config.BucketInterval_,
			p.config.BucketsCount,
			rule.limit,
		)
	case inMemoryBackend:
		return NewInMemoryLimiter(p.config.BucketInterval_, p.config.BucketsCount, rule.limit)
	default:
		p.logger.Panicf("unknown limiter backend: %s", p.config.LimiterBackend)
	}

	return nil
}

// Stop ends plugin activity.
func (p *Plugin) Stop() {
	p.cancel()
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if p.isAllowed(event) {
		return pipeline.ActionPass
	}
	return pipeline.ActionDiscard
}

func (p *Plugin) isAllowed(event *pipeline.Event) bool {
	ts := time.Now()

	if len(p.config.TimeField_) != 0 {
		tsValue := event.Root.Dig(p.config.TimeField_...).AsString()
		t, err := time.Parse(p.format, tsValue)
		if err != nil || ts.IsZero() {
			p.logger.Warnf("can't parse field %q using format %s: %s", p.config.TimeField, p.config.TimeFieldFormat, tsValue)
		} else {
			ts = t
		}
	}

	throttleKey := defaultThrottleKey
	if len(p.config.ThrottleField_) > 0 {
		val := event.Root.Dig(p.config.ThrottleField_...).AsString()
		if val != "" {
			throttleKey = val
		}
	}

	for _, rule := range p.rules {
		if !rule.isMatch(event) {
			continue
		}

		p.limiterBuf = append(p.limiterBuf[:0], rule.byteIdxPart...)
		p.limiterBuf = append(p.limiterBuf, throttleKey...)
		limiterKey := pipeline.ByteToStringUnsafe(p.limiterBuf)

		limitersMu.RLock()
		limiter, has := limiters[p.pipeline][limiterKey]
		limitersMu.RUnlock()

		// fast check with read lock
		if !has {
			limitersMu.Lock()
			limiter, has = limiters[p.pipeline][limiterKey]
			// we could already write it between `limitersMu.RUnlock()` and `limitersMu.Lock()`, so we need to check again
			if !has {
				limiterKeyBytes := p.limiterBuf[2:]
				limiter = p.getNewLimiter(
					string(p.config.ThrottleField),
					pipeline.ByteToStringUnsafe(limiterKeyBytes),
					rule,
				)
				// alloc new string before adding new key to map
				limiterKey = string(p.limiterBuf)
				limiters[p.pipeline][limiterKey] = limiter
			}
			limitersMu.Unlock()
		}

		return limiter.isAllowed(event, ts)
	}

	return true
}
