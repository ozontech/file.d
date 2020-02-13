package throttle

import (
	"sync"
	"time"

	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

var (
	defaultThrottleKey = "default"

	// limiters should be shared across pipeline, so lets have a map by namespace and limiter name
	limiters   = map[string]map[string]*limiter{} // todo: cleanup this map?
	limitersMu = &sync.RWMutex{}
)

/*{ introduction
Discards events if pipeline throughput gets higher than a configured threshold.
}*/
type Plugin struct {
	config   *Config
	pipeline string

	limiterBuff []byte
	rules       []*rule
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> Event field which will be used as a key for throttling.
	//> It means that throttling will work separately for events with different keys.
	//> If not set, it's assumed that all events have the same key.
	ThrottleField  cfg.FieldSelector `json:"throttle_field" default:"" parse:"selector"` //*
	ThrottleField_ []string

	//> @3@4@5@6
	//>
	//> Event field which defines the time when event was fired.
	//> It used to detect event throughput in particular time range.
	//> If not set current time will be taken.
	TimeField  cfg.FieldSelector `json:"time_field" default:"time"` //*
	TimeField_ []string

	//> @3@4@5@6
	//>
	//> Defines how to parse time field format.
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano" options:"ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano"` //*

	//> @3@4@5@6
	//>
	//> Default limit of events that plugin allows per `interval`
	DefaultLimit int64 `json:"default_limit" default:"5000"` //*

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
	//> Rules can override `default_limit` for different group of event. It's a list of objects.
	//> Each object have `limit` and `conditions` field.
	//> * `limit` – value which will override `default_limit`, if `conditions` are met.
	//> * `conditions` – a map of `event field name => event field value`. Conditions are checked using `AND` operator.
	Rules []RuleConfig `json:"rules" default:""` //*
}

type RuleConfig struct {
	Limit      int64             `json:"limit"`
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
	p.limiterBuff = make([]byte, 0, 0)

	limitersMu.Lock()
	limiters[p.pipeline] = map[string]*limiter{}
	limitersMu.Unlock()

	for _, r := range p.config.Rules {
		p.rules = append(p.rules, NewRule(r.Conditions, r.Limit))
	}

	p.rules = append(p.rules, NewRule(map[string]string{}, p.config.DefaultLimit))
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if p.isAllowed(event) {
		return pipeline.ActionPass
	} else {
		return pipeline.ActionDiscard
	}
}

func (p *Plugin) isAllowed(event *pipeline.Event) bool {
	tsValue := event.Root.Dig(p.config.TimeField_...).AsString()
	ts, err := time.Parse(p.config.TimeFieldFormat, tsValue)
	if err != nil || ts.IsZero() {
		ts = time.Now()
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

		if !has {
			limiter = NewLimiter(p.config.BucketInterval_, p.config.BucketsCount, rule.limit)
			// alloc new string before adding new key to map
			limiterKey = string(p.limiterBuff)
			limitersMu.Lock()
			limiters[p.pipeline][limiterKey] = limiter
			limitersMu.Unlock()
		}

		return limiter.isAllowed(ts)
	}

	return true
}
