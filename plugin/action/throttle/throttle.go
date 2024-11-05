package throttle

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
)

const defaultThrottleKey = "default"

var (
	// limiters should be shared across pipeline, so let's have a map by namespace and limiter name
	limiters   = map[string]*limitersMap{}
	limitersMu = &sync.RWMutex{}
)

const (
	limitKindCount = "count"
	limitKindSize  = "size"

	redisBackend    = "redis"
	inMemoryBackend = "memory"
)

/*{ introduction
It discards the events if pipeline throughput gets higher than a configured threshold.
}*/

type Plugin struct {
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *zap.SugaredLogger
	config   *Config
	pipeline string
	format   string

	limitersMap *limitersMap
	limiterBuf  []byte
	rules       []*rule

	// plugin metrics
	limitersMapSizeMetric prometheus.Gauge
	limitDistrMetrics     *limitDistributionMetrics
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
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano"` // *

	// > @3@4@5@6
	// >
	// > The default events limit that plugin allows per `bucket_interval`.
	DefaultLimit int64 `json:"default_limit" default:"5000"` // *

	// > @3@4@5@6
	// >
	// > It defines subject of limiting: number of messages or total size of the messages.
	LimitKind string `json:"limit_kind" default:"count" options:"count|size"` // *

	// > @3@4@5@6
	// >
	// > Defines kind of backend. When redis backend is chosen and if by any reason plugin cannot connect to redis,
	// > limiters will not start syncing with redis until successful reconnect.
	LimiterBackend string `json:"limiter_backend" default:"memory" options:"memory|redis"` // *

	// > @3@4@5@6
	// >
	// > It contains redis settings
	RedisBackendCfg RedisBackendConfig `json:"redis_backend_config" child:"true"` // *

	// > @3@4@5@6
	// >
	// > How much time buckets to hold in the memory. E.g. if `buckets_count` is `60` and `bucket_interval` is `5m`,
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
	// > Each object has the `limit`, `limit_kind` and `conditions` fields as well as an optional `limit_distribution` field.
	// > * `limit` – the value which will override the `default_limit`, if `conditions` are met.
	// > * `limit_kind` – the type of limit: `count` - number of messages, `size` - total size from all messages
	// > * `conditions` – the map of `event field name => event field value`. The conditions are checked using `AND` operator.
	// > * `limit_distribution` – see `LimitDistributionConfig` for details.
	Rules []RuleConfig `json:"rules" default:"" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > Time interval after which unused limiters are removed.
	LimiterExpiration  cfg.Duration `json:"limiter_expiration" parse:"duration" default:"30m"` // *
	LimiterExpiration_ time.Duration

	// > @3@4@5@6
	// >
	// > It allows to distribute the `default_limit` between events by condition.
	// >
	// > `LimitDistributionConfig` params:
	// > * `field` - the event field on which the distribution will be based.
	// > * `ratios` - the list of objects. Each object has:
	// > 	* `ratio` - distribution ratio, value must be in range [0.0;1.0].
	// > 	* `values` - the list of strings which contains all `field` values that fall into this distribution.
	// > * `metric_labels` - list of metric labels.
	// >
	// >> Notes:
	// >> 1. Sum of ratios must be in range [0.0;1.0].
	// >> 2. If sum of ratios less than 1, then adding **default distribution** with ratio **1-sum**,
	// >> otherwise **default distribution** isn't used.
	// >> All events for which the value in the `field` doesn't fall into any of the distributions:
	// >>   * fall into default distribution, if it exists
	// >>   * throttled, otherwise
	// >> 3. **default distribution** can "steal" limit from other distributions after it has exhausted its.
	// >> This is done in order to avoid reserving limits for explicitly defined distributions.
	// >
	// > `LimitDistributionConfig` example:
	// > ```yaml
	// > field: log.level
	// > ratios:
	// >   - ratio: 0.5
	// >     values: ['error']
	// >   - ratio: 0.3
	// >     values: ['warn', 'info']
	// > ```
	// > For this config and the `default_limit=100`:
	// > * events with `log.level=error` will be NO MORE than `50`
	// > * events with `log.level=warn` or `log.level=info` will be NO MORE than `30`
	// > * there will be AT LEAST `20` other events
	// > (can be up to `100` if there are no events with `log.level=error/warn/info`)
	LimitDistribution LimitDistributionConfig `json:"limit_distribution" child:"true"` // *
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

	// > @3@4@5@6
	// >
	// > Defines redis maximum number of retries. If set to 0, no retries will happen.
	MaxRetries int `json:"max_retries" default:"3"` // *

	// > @3@4@5@6
	// >
	// > Defines redis minimum backoff between each retry. If set to 0, sets default 8ms. If set to -1, disables backoff.
	MinRetryBackoff  cfg.Duration `json:"min_retry_backoff" parse:"duration" default:"8ms"` // *
	MinRetryBackoff_ time.Duration

	// > @3@4@5@6
	// >
	// > Defines redis maximum backoff between each retry. If set to 0, sets default 512ms. If set to -1, disables backoff.
	MaxRetryBackoff  cfg.Duration `json:"max_retry_backoff" parse:"duration" default:"512ms"` // *
	MaxRetryBackoff_ time.Duration

	// > @3@4@5@6
	// >
	// > Defines the event field from which values are used as limiter keys. Serves as an override of the default limiter keys naming pattern.
	// > If not set limiter keys are formed using pipeline name, throttle field and throttle field value.
	LimiterKeyField  cfg.FieldSelector `json:"limiter_key_field" default:"" parse:"selector"` // *
	LimiterKeyField_ []string

	// > @3@4@5@6
	// >
	// > Defines field with limit inside json object stored in value
	// > (e.g. if set to "limit", values must be of kind `{"limit":"<int>",...}`).
	// > If not set limiter values are considered as non-json data.
	LimiterValueField string `json:"limiter_value_field" default:""` // *

	// > @3@4@5@6
	// >
	// > Defines field with limit distribution inside json object stored in value
	// > (e.g. if set to "distribution", value must be of kind `{"distribution":{<object>},...}`).
	// > Distribution object example:
	// > ```json
	// > {
	// >   "field": "log.level",
	// >   "ratios": [
	// >     {
	// >       "ratio": 0.5,
	// >       "values": ["error"]
	// >     },
	// >     {
	// >       "ratio": 0.3,
	// >       "values": ["warn", "info"]
	// >     }
	// >   ],
	// >   "enabled": true
	// > }
	// > ```
	// >> If `limiter_value_field` and `limiter_distribution_field` not set, distribution will not be stored.
	LimiterDistributionField string `json:"limiter_distribution_field" default:""` // *
}

type RuleConfig struct {
	Limit             int64                   `json:"limit"`
	LimitKind         string                  `json:"limit_kind" default:"count" options:"count|size"`
	Conditions        map[string]string       `json:"conditions"`
	LimitDistribution LimitDistributionConfig `json:"limit_distribution" child:"true"`
}

type ComplexRatio struct {
	Ratio  float64  `json:"ratio" required:"true"`
	Values []string `json:"values" required:"true"`
}

type LimitDistributionConfig struct {
	Field        cfg.FieldSelector `json:"field" default:""`
	Ratios       []ComplexRatio    `json:"ratios" slice:"true"`
	MetricLabels []string          `json:"metric_labels" slice:"true"`
}

func (c LimitDistributionConfig) toInternal() limitDistributionCfg {
	internal := limitDistributionCfg{
		Field:   string(c.Field),
		Ratios:  make([]limitDistributionRatio, 0, len(c.Ratios)),
		Enabled: true,
	}
	for _, v := range c.Ratios {
		internal.Ratios = append(internal.Ratios, limitDistributionRatio(v))
	}
	return internal
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
	p.logger = params.Logger

	distrCfg := p.config.LimitDistribution.toInternal()
	ld, err := parseLimitDistribution(distrCfg, p.config.DefaultLimit)
	if err != nil {
		p.logger.Fatal("can't parse limit_distribution", zap.Error(err))
	}

	p.registerMetrics(params.MetricCtl, p.config.LimitDistribution.MetricLabels)

	p.pipeline = params.PipelineName
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel

	format, err := pipeline.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.config.TimeFieldFormat
	}
	p.format = format

	limitersMu.Lock()
	// init limitersMap only once per pipeline
	if _, has := limiters[p.pipeline]; !has {
		var redisOpts *redis.Options
		if p.config.LimiterBackend == redisBackend {
			if p.config.RedisBackendCfg.WorkerCount < 1 {
				p.logger.Fatalf("workers_count must be > 0, passed: %d", p.config.RedisBackendCfg.WorkerCount)
			}

			redisOpts = &redis.Options{
				Network:         "tcp",
				Addr:            p.config.RedisBackendCfg.Endpoint,
				Password:        p.config.RedisBackendCfg.Password,
				ReadTimeout:     p.config.RedisBackendCfg.Timeout_,
				WriteTimeout:    p.config.RedisBackendCfg.Timeout_,
				MaxRetries:      p.config.RedisBackendCfg.MaxRetries,
				MinRetryBackoff: p.config.RedisBackendCfg.MinRetryBackoff_,
				MaxRetryBackoff: p.config.RedisBackendCfg.MaxRetryBackoff_,
			}
		}
		lmCfg := limitersMapConfig{
			ctx:                p.ctx,
			limitersExpiration: p.config.LimiterExpiration_,
			isStrict:           params.PipelineSettings.IsStrict,
			logger:             p.logger,
			limiterCfg: &limiterConfig{
				ctx:                      p.ctx,
				backend:                  p.config.LimiterBackend,
				pipeline:                 p.pipeline,
				throttleField:            string(p.config.ThrottleField),
				bucketInterval:           p.config.BucketInterval_,
				bucketsCount:             p.config.BucketsCount,
				limiterValueField:        p.config.RedisBackendCfg.LimiterValueField,
				limiterDistributionField: p.config.RedisBackendCfg.LimiterDistributionField,
			},
			mapSizeMetric:     p.limitersMapSizeMetric,
			limitDistrMetrics: p.limitDistrMetrics,
		}
		limiters[p.pipeline] = newLimitersMap(lmCfg, redisOpts)
		if p.config.LimiterBackend == redisBackend {
			// run sync only once per pipeline
			go limiters[p.pipeline].runSync(p.ctx,
				p.config.RedisBackendCfg.WorkerCount,
				p.config.RedisBackendCfg.SyncInterval_,
			)
		}
		go limiters[p.pipeline].maintenance(p.ctx)
	}
	p.limitersMap = limiters[p.pipeline]
	limitersMu.Unlock()

	for i, r := range p.config.Rules {
		ruleDistrCfg := r.LimitDistribution.toInternal()
		ldRule, err := parseLimitDistribution(ruleDistrCfg, r.Limit)
		if err != nil {
			p.logger.Fatal("can't parse rule's limit_distribution", zap.Error(err), zap.Int("rule", i))
		}

		p.rules = append(p.rules, newRule(
			r.Conditions,
			complexLimit{r.Limit, r.LimitKind, ldRule},
			ruleDistrCfg.marshalJson(),
			i,
		))
	}

	p.rules = append(p.rules, newRule(
		map[string]string{},
		complexLimit{p.config.DefaultLimit, p.config.LimitKind, ld},
		distrCfg.marshalJson(),
		len(p.config.Rules),
	))
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl, limitDistrMetricsLabels []string) {
	p.limitersMapSizeMetric = ctl.RegisterGauge(
		"throttle_limiter_map_size",
		"Size of internal map of throttle limiters",
	)

	labels := []string{"distribution_value"}
	labels = append(labels, limitDistrMetricsLabels...)
	p.limitDistrMetrics = &limitDistributionMetrics{
		CustomLabels: limitDistrMetricsLabels,
		EventsCount: metric.NewHeldCounterVec(ctl.RegisterCounterVec(
			"throttle_distributed_events_count_total",
			"total count of events that have been throttled using limit distribution",
			labels...,
		)),
		EventsSize: metric.NewHeldCounterVec(ctl.RegisterCounterVec(
			"throttle_distributed_events_size_total",
			"total size of events that have been throttled using limit distribution",
			labels...,
		)),
	}
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
	var ts time.Time

	if len(p.config.TimeField_) != 0 {
		tsValue := event.Root.Dig(p.config.TimeField_...).AsString()
		t, err := pipeline.ParseTime(p.format, tsValue)
		if err != nil || t.IsZero() {
			p.logger.Warnf("can't parse field %q using format %s: %s", p.config.TimeField, p.config.TimeFieldFormat, tsValue)
			ts = time.Now()
		} else {
			ts = t
		}
	} else {
		ts = time.Now()
	}

	throttleKey := defaultThrottleKey
	if len(p.config.ThrottleField_) > 0 {
		val := event.Root.Dig(p.config.ThrottleField_...).AsString()
		if val != "" {
			throttleKey = val
		}
	}
	keyLimitOverride := ""
	if len(p.config.RedisBackendCfg.LimiterKeyField_) > 0 {
		keyLimitOverride = strings.Clone(event.Root.Dig(p.config.RedisBackendCfg.LimiterKeyField_...).AsString())
	}

	for _, r := range p.rules {
		if !r.isMatch(event) {
			continue
		}
		var lim limiter
		lim, p.limiterBuf = p.limitersMap.getOrAdd(throttleKey, keyLimitOverride, p.limiterBuf, r)
		return lim.isAllowed(event, ts)
	}

	return true
}
