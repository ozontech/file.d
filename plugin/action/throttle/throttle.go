package throttle

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

var (
	defaultThrottleKey = "default"

	// limiters should be shared across pipeline, so let's have a map by namespace and limiter name
	limiters   = map[string]map[string]limiter{} // todo: cleanup this map?
	limitersMu = &sync.RWMutex{}
)

const (
	redisLimiterType    = "redis"
	inMemoryLimiterType = "memory"
)

/*{ introduction
It discards the events if pipeline throughput gets higher than a configured threshold.
}*/
type Plugin struct {
	config   *Config
	pipeline string

	redisClient redisClient

	limiterBuff []byte
	rules       []*rule
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The event field which will be used as a key for throttling.
	//> It means that throttling will work separately for events with different keys.
	//> If not set, it's assumed that all events have the same key.
	ThrottleField  cfg.FieldSelector `json:"throttle_field" default:"" parse:"selector"` //*
	ThrottleField_ []string

	//> @3@4@5@6
	//>
	//> The event field which defines the time when event was fired.
	//> It is used to detect the event throughput in a particular time range.
	//> If not set, the current time will be taken.
	TimeField  cfg.FieldSelector `json:"time_field" default:"time"` //*
	TimeField_ []string

	//> @3@4@5@6
	//>
	//> It defines how to parse the time field format.
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"` //*

	//> @3@4@5@6
	//>
	//> The default events limit that plugin allows per `interval`
	DefaultLimit int64 `json:"default_limit" default:"5000"` //*

	//> @3@4@5@6
	//>
	//> What we're limiting: number of messages, or total size of the messages
	LimitKind string `json:"limit_kind" default:"count" options:"count|size"` //*

	//> @3@4@5@6
	//>
	//> Where do we store the limit: locally in memory or in remote redis
	LimiterKind string `json:"limiter_kind" default:"memory" options:"memory|redis"`

	//> @3@4@5@6
	//>
	//> Host:port of redis server
	RedisHost string `json:"redis_host"`

	//> @3@4@5@6
	//>
	//> RedisPassword
	RedisPassword string `json:"redis_pass"`

	//> @3@4@5@6
	//>
	//> How ofter try to sync global threshold and limit values
	RedisRefreshInterval  cfg.Duration `json:"redis_refresh_interval" parse:"duration" default:"5s"`
	RedisRefreshInterval_ time.Duration

	//> @3@4@5@6
	//>
	//> How much time buckets to hold in the memory. E.g. if `buckets_count` is `60` and `interval` is `5m`,
	//> then `5 hours` will be covered. Events with time later than `now() - 5h` will be dropped even if threshold isn't exceeded.
	BucketsCount int `json:"buckets_count" default:"60"` //*

	//> @3@4@5@6
	//>
	//> Time interval to check event throughput.
	BucketInterval  cfg.Duration `json:"bucket_interval" parse:"duration" default:"1m"` //*
	BucketInterval_ time.Duration

	//> @3@4@5@6
	//>
	//> Rules to override the `default_limit` for different group of event. It's a list of objects.
	//> Each object has the `limit` and `conditions` fields.
	//> * `limit` – the value which will override the `default_limit`, if `conditions` are met.
	//> * `limit_kind` – the type of a limit: `count` - number of messages, `size` - total size from all messages
	//> * `conditions` – the map of `event field name => event field value`. The conditions are checked using `AND` operator.
	Rules []RuleConfig `json:"rules" default:"" slice:"true"` //*

	//> @3@4@5@6
	//>
	//> IsThrottleDistributed regulates global tracing for service.
	IsThrottleDistributed bool `json:"is_throttle_distributed" default:"false"` //*
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.pipeline = params.PipelineName
	p.limiterBuff = make([]byte, 0)

	limitersMu.Lock()
	limiters[p.pipeline] = map[string]limiter{}
	limitersMu.Unlock()

	if p.config.LimiterKind == redisLimiterType {
		p.redisClient = redis.NewClient(&redis.Options{
			Network:  "tcp",
			Addr:     p.config.RedisHost,
			Password: p.config.RedisPassword,
		})

		pingResp := p.redisClient.Ping()
		if pingResp.Err() != nil {
			logger.Fatalf("Can't ping redis: %s", pingResp.Err())
		}
	}

	for _, r := range p.config.Rules {
		p.rules = append(p.rules, NewRule(r.Conditions, complexLimit{r.Limit, r.LimitKind}))
	}

	p.rules = append(p.rules, NewRule(map[string]string{}, complexLimit{p.config.DefaultLimit, p.config.LimitKind}))
}

func (p *Plugin) getNewLimiter(throttleField, limiterKey string, rule *rule) limiter {
	switch p.config.LimiterKind {
	case redisLimiterType:
		return NewRedisLimiter(
			p.redisClient,
			throttleField,
			limiterKey,
			p.config.RedisRefreshInterval_,
			p.config.BucketInterval_,
			p.config.BucketsCount,
			rule.limit,
			p.config.IsThrottleDistributed,
		)
	case inMemoryLimiterType:
		fallthrough
	default:
		return NewInMemoryLimiter(p.config.BucketInterval_, p.config.BucketsCount, rule.limit)
	}
}

func (p *Plugin) Stop() {
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
		t, err := time.Parse(p.config.TimeFieldFormat, tsValue)
		if err != nil || ts.IsZero() {
			logger.Warnf("can't parse field %q using format %s: %s", p.config.TimeField, p.config.TimeFieldFormat, tsValue)
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

	for index, rule := range p.rules {
		if !rule.isMatch(event) {
			continue
		}

		p.limiterBuff = append(p.limiterBuff[:0], byte('a'+index))
		p.limiterBuff = append(p.limiterBuff, ':')
		p.limiterBuff = append(p.limiterBuff, throttleKey...)
		limiterKey := pipeline.ByteToStringUnsafe(p.limiterBuff)

		// check if limiter already have been created
		limitersMu.RLock()
		limiter, has := limiters[p.pipeline][limiterKey]
		limitersMu.RUnlock()

		// fast check with read lock
		if !has {
			limitersMu.Lock()
			limiter, has = limiters[p.pipeline][limiterKey]
			// we could already write it between `limitersMu.RUnlock()` and `limitersMu.Lock()`, so we need to check again
			if !has {
				limiter = p.getNewLimiter(
					string(p.config.ThrottleField),
					pipeline.ByteToStringUnsafe(p.limiterBuff[2:]),
					rule,
				)
				// alloc new string before adding new key to map
				limiterKey = string(p.limiterBuff)
				limiters[p.pipeline][limiterKey] = limiter
			}
			limitersMu.Unlock()
		}

		return limiter.isAllowed(event, ts)
	}

	return true
}
