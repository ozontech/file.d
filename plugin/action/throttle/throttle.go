package throttle

import (
	"bytes"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

var (
	defaultThrottleKey        = "default"
	defaultTimeField          = "time"
	defaultDefaultLimit int64 = 10000

	// limiters should be shared across pipeline, so lets have a map by namespace and limiter name
	limiters   = map[string]map[string]*limiter{} // todo: cleanup this map?
	limitersMu = &sync.RWMutex{}
)

// Plugin throttle plugin drops events if event count per interval gets higher than a configured threshold
type Plugin struct {
	config   *Config
	pipeline string

	limiterBuff *bytes.Buffer
	rules       []*rule
}

type Config struct {
	ThrottleField string            `json:"throttle_field"`
	TimeField     string            `json:"time_field"`
	DefaultLimit  int64             `json:"default_limit"`
	Interval      pipeline.Duration `json:"interval"`
	Buckets       int               `json:"buckets"`
	Rules         []RuleConfig      `json:"rules"`
}

type RuleConfig struct {
	Limit      int64             `json:"limit"`
	Conditions map[string]string `json:"conditions"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
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
	p.limiterBuff = bytes.NewBufferString("")

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

func (p *Plugin) Reset() {
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

		p.limiterBuff.Reset()
		p.limiterBuff.WriteByte(byte('a' + index))
		p.limiterBuff.WriteByte(':')
		p.limiterBuff.WriteString(throttleKey)
		limiterKey := p.limiterBuff.String()

		// check if limiter already have been created
		limitersMu.RLock()
		limiter, has := limiters[p.pipeline][limiterKey]
		limitersMu.RUnlock()

		if !has {
			limiter = NewLimiter(p.config.Interval.Duration, p.config.Buckets, rule.limit)
			limitersMu.Lock()
			limiters[p.pipeline][limiterKey] = limiter
			limitersMu.Unlock()
		}

		return limiter.isAllowed(ts)
	}

	return true
}
