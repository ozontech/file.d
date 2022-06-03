package backoff

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/stats"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	stats.InitStats()

	tCases := []struct {
		name            string
		opts            []Option
		counter         prom.Counter
		timeout         time.Duration
		retriesCfg      RetriesCfg
		expParams       []interface{}
		expInitInterval time.Duration
		expRandFactor   float64
		expMulti        float64
		expMaxInterval  time.Duration
	}{
		{
			name: "initial_interval_opt",
			opts: []Option{
				InitialIntervalOpt(time.Nanosecond * 20),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: time.Nanosecond * 20,
			expRandFactor:   backoff.DefaultRandomizationFactor,
			expMulti:        backoff.DefaultMultiplier,
			expMaxInterval:  backoff.DefaultMaxInterval,
		},
		{
			name: "initial_interval_opt",
			opts: []Option{
				InitialIntervalOpt(time.Nanosecond * 20),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: time.Nanosecond * 20,
			expRandFactor:   backoff.DefaultRandomizationFactor,
			expMulti:        backoff.DefaultMultiplier,
			expMaxInterval:  backoff.DefaultMaxInterval,
		},
		{
			name: "randomization_factor_opt",
			opts: []Option{
				RandomizationFactor(5.5),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: backoff.DefaultInitialInterval,
			expRandFactor:   5.5,
			expMulti:        backoff.DefaultMultiplier,
			expMaxInterval:  backoff.DefaultMaxInterval,
		},
		{
			name: "multiplier_opt",
			opts: []Option{
				Multiplier(4.4),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: backoff.DefaultInitialInterval,
			expRandFactor:   backoff.DefaultRandomizationFactor,
			expMulti:        4.4,
			expMaxInterval:  backoff.DefaultMaxInterval,
		},
		{
			name: "max_interval_opt",
			opts: []Option{
				MaxInterval(time.Nanosecond * 44),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: backoff.DefaultInitialInterval,
			expRandFactor:   backoff.DefaultRandomizationFactor,
			expMulti:        backoff.DefaultMultiplier,
			expMaxInterval:  time.Nanosecond * 44,
		},
		{
			name: "all_opt",
			opts: []Option{
				InitialIntervalOpt(time.Nanosecond * 20),
				RandomizationFactor(2.2),
				Multiplier(8.8),
				MaxInterval(time.Microsecond * 3),
			},
			counter:         stats.GetCounter("random", "random"),
			timeout:         time.Second,
			retriesCfg:      RetriesCfg{Limited: false, Limit: 0},
			expInitInterval: time.Nanosecond * 20,
			expRandFactor:   2.2,
			expMulti:        8.8,
			expMaxInterval:  time.Microsecond * 3,
		},
	}

	for _, tCase := range tCases {
		t.Run(tCase.name, func(t *testing.T) {
			expBackoff := New(context.Background(), tCase.counter, tCase.timeout, tCase.retriesCfg, tCase.opts...)
			backoffCtx := expBackoff.backoff.(backoff.BackOffContext)

			// get backoff.ExponentialBackoff under unexportable backoff.backOffContext
			underlyingBackoff := reflect.Indirect(reflect.ValueOf(backoffCtx)).FieldByName("BackOff").Interface().(*backoff.ExponentialBackOff)
			require.Equal(t, tCase.expInitInterval, underlyingBackoff.InitialInterval)
			require.Equal(t, tCase.expRandFactor, underlyingBackoff.RandomizationFactor)
			require.Equal(t, tCase.expMulti, underlyingBackoff.Multiplier)
			require.Equal(t, tCase.expMaxInterval, underlyingBackoff.MaxInterval)
		})
	}
}

func TestNewWithRetries(t *testing.T) {
	limit := uint64(10)
	expBackoff := New(context.Background(), nil, time.Nanosecond, RetriesCfg{Limited: true, Limit: limit})
	tries := reflect.Indirect(reflect.ValueOf(expBackoff.backoff)).FieldByName("maxTries").Uint()

	require.Equal(t, limit, tries)
}

func TestExec(t *testing.T) {
	stats.InitStats()
	stats.RegisterCounter(&stats.MetricDesc{
		Subsystem: "backoff_subsys",
		Name:      "backoff_cnt_test",
		Help:      "For tests",
	})
	counter := stats.GetCounter("backoff_subsys", "backoff_cnt_test")

	ctx := context.Background()

	retry := 10
	expBackoff := New(context.Background(), counter, time.Second, RetriesCfg{Limited: true, Limit: uint64(retry)})

	var wg sync.WaitGroup
	wg.Add(1)
	executor := func(context.Context) error {
		defer wg.Done()
		return nil
	}

	var err error
	func() {
		err = expBackoff.RetryWithMetrics(ctx, executor)
	}()

	wg.Wait()
	require.NoError(t, err)
}

func TestExecError(t *testing.T) {
	stats.InitStats()
	stats.RegisterCounter(&stats.MetricDesc{
		Subsystem: "backoff_subsys",
		Name:      "backoff_cnt_test",
		Help:      "For tests",
	})
	counter := stats.GetCounter("backoff_subsys", "backoff_cnt_test")

	expErr := errors.New("some error")

	ctx := context.Background()
	retry := 10
	expBackoff := New(
		context.Background(),
		counter,
		time.Second,
		RetriesCfg{Limited: true, Limit: uint64(retry) - 1},
		InitialIntervalOpt(time.Nanosecond),
		MaxInterval(time.Nanosecond),
	)

	var wg sync.WaitGroup
	wg.Add(retry)
	executor := func(context.Context) error {
		defer wg.Done()
		return expErr
	}

	var err error
	func() {
		err = expBackoff.RetryWithMetrics(ctx, executor)
	}()

	wg.Wait()
	require.Error(t, err)
	require.EqualError(t, expErr, err.Error())
}

func TestExecSuccessAfterRetry(t *testing.T) {
	stats.InitStats()
	stats.RegisterCounter(&stats.MetricDesc{
		Subsystem: "backoff_subsys",
		Name:      "backoff_cnt_test",
		Help:      "For tests",
	})
	counter := stats.GetCounter("backoff_subsys", "backoff_cnt_test")

	expErr := errors.New("some error")

	ctx := context.Background()

	expBackoff := New(
		context.Background(),
		counter,
		time.Second,
		RetriesCfg{Limited: false},
		InitialIntervalOpt(time.Nanosecond),
		MaxInterval(time.Nanosecond),
	)

	successAfter := 10
	i := 0
	var wg sync.WaitGroup
	wg.Add(successAfter + 1)
	executor := func(context.Context) error {
		defer wg.Done()

		if i >= successAfter {
			return nil
		}
		i++
		return expErr
	}

	var err error
	func() {
		err = expBackoff.RetryWithMetrics(ctx, executor)
	}()

	wg.Wait()
	require.NoError(t, err)
}
