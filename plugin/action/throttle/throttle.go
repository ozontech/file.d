package throttle

import (
	"sync"
	"time"

	"gitlab.ozon.ru/sre/file-d/config"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

var (
	defaultThrottleKey        = "default"
	defaultTimeField          = "time"
	defaultDefaultLimit int64 = 10000

	// limiters should be shared across pipeline, so lets have a map by namespace and limiter name
	limiters   = map[string]map[string]*limiter{} // todo: cleanup this map?
	limitersMu = &sync.RWMutex{}
)

/*{ introduction
Plugin throttle plugin drops events if event count per interval gets higher than a configured threshold.
}*/
type Plugin struct {
	config   *Config
	pipeline string

	limiterBuff []byte
	rules       []*rule
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> To be filled
	ThrottleField string `json:"throttle_field"` //*

	//> @3 @4 @5 @6
	//>
	//> To be filled
	TimeField string `json:"time_field"` //*

	//> @3 @4 @5 @6
	//>
	//> To be filled
	DefaultLimit int64 `json:"default_limit"` //*

	//> @3 @4 @5 @6
	//>
	//> To be filled
	Interval  cfg.Duration `json:"interval" parse:"duration"` //*
	Interval_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> To be filled
	Buckets int `json:"buckets"` //*

	//> @3 @4 @5 @6
	//>
	//> To be filled
	Rules []RuleConfig `json:"rules"` //*
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

	if p.config.DefaultLimit == 0 {
		p.config.DefaultLimit = defaultDefaultLimit
	}

	if p.config.ThrottleField == "" {
		logger.Fatalf("throttle_field isn't set for throttle plugin")
	}

	if p.config.TimeField == "" {
		p.config.TimeField = defaultTimeField
	}

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
	tsValue := event.Root.Dig(p.config.TimeField).AsString()
	ts, err := time.Parse(time.RFC3339Nano, tsValue)
	if err != nil || ts.IsZero() {
		ts = time.Now()
	}

	throttleKey := event.Root.Dig(p.config.ThrottleField).AsString()
	if throttleKey == "" {
		throttleKey = defaultThrottleKey
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
			limiter = NewLimiter(p.config.Interval_, p.config.Buckets, rule.limit)
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
