package throttle

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/pipeline"
)

const maintenanceInterval = time.Second

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

// limiterWithGen is a wrapper for the limiter interface with added generation field.
type limiterWithGen struct {
	limiter
	gen *atomic.Int64
}

func newLimiterWithGen(lim limiter, gen int64) *limiterWithGen {
	return &limiterWithGen{
		limiter: lim,
		gen:     atomic.NewInt64(gen),
	}
}

// limiterConfig configuration for creation of new limiters.
type limiterConfig struct {
	ctx               context.Context
	backend           string
	redisClient       redisClient
	pipeline          string
	throttleField     string
	bucketInterval    time.Duration
	bucketsCount      int
	limiterValueField string
}

// limitersMapConfig configuration of limiters map.
type limitersMapConfig struct {
	limitersExpiration time.Duration
	logger             *zap.SugaredLogger

	limiterCfg *limiterConfig

	mapSizeMetric *prom.GaugeVec
}

// limitersMap is auxiliary type for storing the map of strings to limiters with additional info for cleanup
// and thread safe map operations handling.
type limitersMap struct {
	lims        map[string]*limiterWithGen
	mu          *sync.RWMutex
	limiterBuf  []byte
	activeTasks atomic.Uint32
	curGen      int64
	limitersExp int64
	logger      *zap.SugaredLogger

	limiterCfg *limiterConfig

	mapSizeMetric *prom.GaugeVec
}

func newLimitersMap(lmCfg limitersMapConfig, redisOpts *redis.Options) *limitersMap {
	nowTs := time.Now().UnixMicro()
	lm := &limitersMap{
		lims:        make(map[string]*limiterWithGen),
		mu:          &sync.RWMutex{},
		limiterBuf:  make([]byte, 0),
		activeTasks: *atomic.NewUint32(0),
		curGen:      nowTs,
		limitersExp: lmCfg.limitersExpiration.Microseconds(),
		logger:      lmCfg.logger,

		limiterCfg: lmCfg.limiterCfg,

		mapSizeMetric: lmCfg.mapSizeMetric,
	}
	if redisOpts != nil {
		client := redis.NewClient(redisOpts)
		if pingResp := client.Ping(); pingResp.Err() != nil {
			lm.logger.Errorf("can't ping redis: %s", pingResp.Err())
			lm.logger.Warn("couldn't connect to redis, falling back to in-memory limiters")
		} else {
			lm.limiterCfg.redisClient = client
		}
	}
	return lm
}

func (l *limitersMap) syncWorker(jobCh <-chan limiter, wg *sync.WaitGroup) {
	for job := range jobCh {
		job.sync()
		wg.Done()
	}
}

// runSync starts procedure of limiters sync with workers count and interval set in limitersMap.
func (l *limitersMap) runSync(ctx context.Context, workerCount int, syncInterval time.Duration) {
	wg := sync.WaitGroup{}

	jobs := make(chan limiter, workerCount)
	for i := 0; i < workerCount; i++ {
		go l.syncWorker(jobs, &wg)
	}

	ticker := time.NewTicker(syncInterval)
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			return
		case <-ticker.C:
			l.mu.RLock()
			for _, lim := range l.lims {
				wg.Add(1)
				jobs <- lim
			}
			wg.Wait()
			l.mu.RUnlock()
		}
	}
}

// maintenance performs map cleanup.
//
// The map cleanup is executed using timestamp generations: current generation of the map is updated every sync to the
// current time in microseconds, every limiter in the map has its own generation indicating last time it was acquired.
// If the difference between the current generation of the map and the limiter's generation is greater than the
// limiters expiration, the limiter's key is deleted from the map.
func (l *limitersMap) maintenance() {
	for {
		time.Sleep(maintenanceInterval)
		l.mu.Lock()
		// find expired limiters and remove them
		nowTs := time.Now().UnixMicro()
		l.curGen = nowTs
		for key, lim := range l.lims {
			if nowTs-lim.gen.Load() < l.limitersExp {
				continue
			}
			delete(l.lims, key)
		}
		mapSize := float64(len(l.lims))
		l.mapSizeMetric.WithLabelValues().Set(mapSize)
		l.mu.Unlock()
	}
}

// getNewLimiter creates new limiter based on limiter configuration.
func (l *limitersMap) getNewLimiter(throttleKey, keyLimitOverride string, rule *rule) limiter {
	switch l.limiterCfg.backend {
	case redisBackend:
		var lim limiter
		if l.limiterCfg.redisClient != nil {
			lim = NewRedisLimiter(
				l.limiterCfg.ctx,
				l.limiterCfg.redisClient,
				l.limiterCfg.pipeline,
				l.limiterCfg.throttleField,
				throttleKey,
				l.limiterCfg.bucketInterval,
				l.limiterCfg.bucketsCount,
				rule.limit,
				keyLimitOverride,
				l.limiterCfg.limiterValueField,
			)
		} else {
			lim = NewInMemoryLimiter(l.limiterCfg.bucketInterval, l.limiterCfg.bucketsCount, rule.limit)
		}
		return lim
	case inMemoryBackend:
		return NewInMemoryLimiter(l.limiterCfg.bucketInterval, l.limiterCfg.bucketsCount, rule.limit)
	default:
		l.logger.Panicf("unknown limiter backend: %s", l.limiterCfg.backend)
	}
	return nil
}

// getOrAdd tries to get limiter from map by the given key. If key exists, updates it's generation
// to the current limiters map generation and returns limiter. Otherwise creates new limiter,
// sets its generation to the current limiters map generation, adds to map under the given key
// and returns created limiter.
func (l *limitersMap) getOrAdd(throttleKey, keyLimitOverride string, rule *rule) limiter {
	l.limiterBuf = append(l.limiterBuf[:0], rule.byteIdxPart...)
	l.limiterBuf = append(l.limiterBuf, throttleKey...)
	key := string(l.limiterBuf)
	// fast check with read lock
	l.mu.RLock()
	lim, has := l.lims[key]
	if has {
		lim.gen.Store(l.curGen)
		l.mu.RUnlock()
		return lim
	}
	l.mu.RUnlock()
	// we could already write it between `l.mu.RUnlock()` and `l.mu.Lock()`, so we need to check again
	l.mu.Lock()
	lim, has = l.lims[key]
	if has {
		lim.gen.Store(l.curGen)
		l.mu.Unlock()
		return lim
	}
	newLim := l.getNewLimiter(throttleKey, keyLimitOverride, rule)
	lim = newLimiterWithGen(newLim, l.curGen)
	l.lims[key] = lim
	l.mu.Unlock()
	return lim
}
