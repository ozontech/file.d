package throttle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xredis"
)

const (
	maintenanceInterval    = time.Second
	redisReconnectInterval = 30 * time.Minute
	limitsFileTempSuffix   = ".atomic"
)

type limiter interface {
	isAllowed(event *pipeline.Event, ts time.Time) bool
	sync()
	isLimitCfgChanged(curLimit int64, curDistribution []limitDistributionRatio) bool
	getLimitCfg() limitCfg

	// setNowFn is used for testing purposes
	setNowFn(fn func() time.Time)
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
	ctx                      context.Context
	backend                  string
	redisClient              xredis.Client
	pipeline                 string
	throttleField            string
	bucketInterval           time.Duration
	bucketsCount             int
	limiterValueField        string
	limiterDistributionField string
	limitsFile               string
	limitsFileTmp            string
}

type limitCfg struct {
	Key          string               `json:"key"`
	Kind         string               `json:"kind"`
	Limit        int64                `json:"limit"`
	Distribution limitDistributionCfg `json:"distribution"`
}

// limitersMapConfig configuration of limiters map.
type limitersMapConfig struct {
	ctx                context.Context
	limitersExpiration time.Duration
	isStrict           bool
	logger             *zap.SugaredLogger

	limiterCfg *limiterConfig

	// metrics
	mapSizeMetric     *metric.Gauge
	limitDistrMetrics *limitDistributionMetrics
}

// limitersMap is auxiliary type for storing the map of strings to limiters with additional info for cleanup
// and thread safe map operations handling.
type limitersMap struct {
	ctx              context.Context
	lims             map[string]*limiterWithGen
	limsCfg          map[string]limitCfg
	mu               *sync.RWMutex
	activeTasks      atomic.Uint32
	curGen           int64
	limitersExp      int64
	isRedisConnected bool
	logger           *zap.SugaredLogger

	// nowFn is passed to create limiters and required for test purposes
	nowFn func() time.Time

	limiterCfg *limiterConfig

	// metrics
	mapSizeMetric     *metric.Gauge
	limitDistrMetrics *limitDistributionMetrics
}

func newLimitersMap(lmCfg limitersMapConfig, redisOpts *xredis.Options) *limitersMap {
	nowTs := time.Now().UnixMicro()
	lm := &limitersMap{
		ctx:         lmCfg.ctx,
		lims:        make(map[string]*limiterWithGen),
		mu:          &sync.RWMutex{},
		activeTasks: *atomic.NewUint32(0),
		curGen:      nowTs,
		limitersExp: lmCfg.limitersExpiration.Microseconds(),
		logger:      lmCfg.logger,
		nowFn:       time.Now,

		limiterCfg: lmCfg.limiterCfg,

		mapSizeMetric:     lmCfg.mapSizeMetric,
		limitDistrMetrics: lmCfg.limitDistrMetrics,
	}
	if redisOpts != nil {
		lm.limsCfg = make(map[string]limitCfg)
		lm.limiterCfg.limitsFileTmp = lmCfg.limiterCfg.limitsFile + limitsFileTempSuffix

		lm.limiterCfg.redisClient = xredis.NewClient(redisOpts)
		if pingErr := lm.limiterCfg.redisClient.Ping(lm.ctx).Err(); pingErr != nil {
			msg := fmt.Sprintf("can't ping redis: %s", pingErr.Error())
			if lmCfg.isStrict {
				lm.logger.Fatal(msg)
			}
			lm.logger.Error(msg)
			lm.logger.Warnf(
				"sync with redis won't start until successful connect, reconnection attempts will happen every %s",
				redisReconnectInterval,
			)
		} else {
			lm.isRedisConnected = true
			lm.logger.Info("connected to redis")
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

func (l *limitersMap) waitRedisReconnect(ctx context.Context) {
	if l.isRedisConnected {
		return
	}
	ticker := time.NewTicker(redisReconnectInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pingErr := l.limiterCfg.redisClient.Ping(l.ctx).Err(); pingErr == nil {
				l.isRedisConnected = true
				l.logger.Info("connected to redis")
				return
			} else {
				l.logger.Errorf("can't ping redis: %s", pingErr.Error())
			}
		}
	}
}

// runSync starts procedure of limiters sync with workers count and interval set in limitersMap.
func (l *limitersMap) runSync(ctx context.Context, workerCount int, syncInterval time.Duration) {
	// if redis failed to connect, wait for successful reconnect before starting workers
	l.waitRedisReconnect(ctx)
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}

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
func (l *limitersMap) maintenance(ctx context.Context) {
	ticker := time.NewTicker(maintenanceInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock()
			// find expired limiters and remove them
			nowTs := time.Now().UnixMicro()
			l.curGen = nowTs
			for key, lim := range l.lims {
				if nowTs-lim.gen.Load() < l.limitersExp {
					continue
				}
				delete(l.lims, key)
				delete(l.limsCfg, key)
			}
			mapSize := float64(len(l.lims))
			l.mapSizeMetric.Set(mapSize)
			l.mu.Unlock()
		}
	}
}

// newLimiter creates new limiter based on limiter configuration.
func (l *limitersMap) newLimiter(throttleKey, keyLimitOverride string, rule *rule) limiter {
	switch l.limiterCfg.backend {
	case redisBackend:
		return newRedisLimiter(
			l.limiterCfg,
			throttleKey, keyLimitOverride,
			&rule.limit,
			l.limitDistrMetrics,
			l.nowFn,
		)
	case inMemoryBackend:
		return newInMemoryLimiter(l.limiterCfg, &rule.limit, l.limitDistrMetrics, l.nowFn)
	default:
		l.logger.Panicf("unknown limiter backend: %s", l.limiterCfg.backend)
	}
	return nil
}

// getOrAdd tries to get limiter from map by the given key. If key exists, updates it's generation
// to the current limiters map generation and returns limiter. Otherwise creates new limiter,
// sets its generation to the current limiters map generation, adds to map under the given key
// and returns created limiter.
func (l *limitersMap) getOrAdd(throttleKey, keyLimitOverride string, limiterBuf []byte, rule *rule) (limiter, []byte) {
	limiterBuf = append(limiterBuf[:0], rule.byteIdxPart...)
	limiterBuf = append(limiterBuf, throttleKey...)

	// fast check with read lock
	l.mu.RLock()
	lim, has := l.lims[string(limiterBuf)]
	if has {
		lim.gen.Store(l.curGen)
		l.mu.RUnlock()
		return lim, limiterBuf
	}
	l.mu.RUnlock()
	// copy limiter key, to avoid data races
	key := string(limiterBuf)
	// we could already write it between `l.mu.RUnlock()` and `l.mu.Lock()`, so we need to check again
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, has = l.lims[key]
	if has {
		lim.gen.Store(l.curGen)
		return lim, limiterBuf
	}

	newLim := l.newLimiter(throttleKey, keyLimitOverride, rule)
	lim = newLimiterWithGen(newLim, l.curGen)
	l.lims[key] = lim
	if l.limiterCfg.backend == redisBackend {
		l.limsCfg[key] = lim.getLimitCfg()
	}

	return lim, limiterBuf
}

func (l *limitersMap) saveLimitsCyclic(ctx context.Context, duration time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			l.updateLimitsCfg()
			l.saveLimits()
			time.Sleep(duration)
		}
	}
}

func (l *limitersMap) saveLimits() {
	tmpWithRandom := fmt.Sprintf("%s.%08d", l.limiterCfg.limitsFileTmp, rand.Uint32())

	file, err := os.OpenFile(tmpWithRandom, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		l.logger.Errorf("can't open temp limits file %s, %s", l.limiterCfg.limitsFileTmp, err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			l.logger.Errorf("can't close limits file %s, %s", l.limiterCfg.limitsFileTmp, err.Error())
		}
	}()

	l.mu.RLock()
	buf, err := json.MarshalIndent(l.limsCfg, "", "  ")
	l.mu.RUnlock()
	if err != nil {
		l.logger.Errorf("can't marshall limits map into json: %s", err.Error())
	}

	_, err = file.Write(buf)
	if err != nil {
		l.logger.Errorf("can't write limits file %s, %s", l.limiterCfg.limitsFileTmp, err.Error())
	}

	err = file.Sync()
	if err != nil {
		l.logger.Errorf("can't sync limits file %s, %s", l.limiterCfg.limitsFileTmp, err.Error())
	}

	err = os.Rename(tmpWithRandom, l.limiterCfg.limitsFile)
	if err != nil {
		logger.Errorf("failed renaming temporary limits file to current: %s", err.Error())
	}
}

func (l *limitersMap) updateLimitsCfg() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for key, limiter := range l.lims {
		if limiter.isLimitCfgChanged(l.limsCfg[key].Limit, l.limsCfg[key].Distribution.Ratios) {
			l.limsCfg[key] = limiter.getLimitCfg()
		}
	}
}

func (l *limitersMap) loadLimits() error {
	info, err := os.Stat(l.limiterCfg.limitsFile)
	if os.IsNotExist(err) {
		return nil
	}

	if info.IsDir() {
		return fmt.Errorf("file %s is dir", l.limiterCfg.limitsFile)
	}

	data, err := os.ReadFile(l.limiterCfg.limitsFile)
	if err != nil {
		return fmt.Errorf("can't read limits file: %w", err)
	}

	err = l.parseLimits(data)
	if err != nil {
		return fmt.Errorf("can't parse limits: %w", err)
	}

	return nil
}

func (l *limitersMap) parseLimits(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if err := json.Unmarshal(data, &l.limsCfg); err != nil {
		return fmt.Errorf("can't unmarshal map: %w", err)
	}

	for key, cfg := range l.limsCfg {
		throttleKey := key[strings.IndexByte(key, ':')+1:]

		distr, err := parseLimitDistribution(cfg.Distribution, cfg.Limit)
		if err != nil {
			return fmt.Errorf("can't parse limit_distribution: %w", err)
		}

		newLim := newRedisLimiter(
			l.limiterCfg,
			throttleKey, cfg.Key,
			&complexLimit{
				value:         cfg.Limit,
				kind:          cfg.Kind,
				distributions: distr,
			},
			l.limitDistrMetrics,
			l.nowFn,
		)
		lim := newLimiterWithGen(newLim, l.curGen)
		l.lims[key] = lim
	}

	return nil
}

// setNowFn is used for testing purposes. Sets custom now func.
// If propagate flag is true, sets the given nowFn to all existing limiters in map.
func (l *limitersMap) setNowFn(nowFn func() time.Time, propagate bool) {
	l.mu.Lock()
	l.nowFn = nowFn
	if propagate {
		for _, lim := range l.lims {
			lim.setNowFn(nowFn)
		}
	}
	l.mu.Unlock()
}
