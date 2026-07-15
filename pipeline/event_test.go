package pipeline

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventPoolDump(t *testing.T) {
	eventPool := newTestEventPool(2)
	e := eventPool.get(1)
	defer eventPool.back(e)

	require.NotPanics(t, func() {
		_ = eventPool.dump()
	})
}

func BenchmarkEventPoolOneGoroutine(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		for range b.N {
			p.back(p.get(1))
		}
	}
	const capacity = 32
	b.Run("eventPool", func(b *testing.B) {
		p := newTestEventPool(capacity)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newTestLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func BenchmarkEventPoolManyGoroutines(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		workers := runtime.GOMAXPROCS(0)
		for range b.N {
			wg := &sync.WaitGroup{}
			wg.Add(workers)
			for range workers {
				go func() {
					defer wg.Done()

					for range 1000 {
						p.back(p.get(1))
					}
				}()
			}
			wg.Wait()
		}
	}
	const capacity = 32
	b.Run("eventPool", func(b *testing.B) {
		p := newTestEventPool(capacity)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newTestLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func BenchmarkEventPoolSlowestPath(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		wg := &sync.WaitGroup{}
		for range b.N {
			const concurrency = 1_000
			wg.Add(concurrency)
			for range concurrency {
				go func() {
					defer wg.Done()
					e := p.get(1)
					p.back(e)
				}()
			}
			wg.Wait()
		}
	}

	const capacity = 32
	b.Run("eventPool", func(b *testing.B) {
		p := newTestEventPool(capacity)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newTestLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func TestLowMemPool(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	test := func(capacity, batchSize int) {
		p := newTestLowMemoryEventPool(capacity)
		for range batchSize {
			batch := make([]*Event, batchSize)
			for j := range batchSize {
				batch[j] = p.get(1)
			}
			r.Equal(int64(batchSize), p.inUse())
			r.Equal(int64(0), p.waiters())

			for j := range batchSize {
				p.back(batch[j])
			}
		}
	}

	test(1, 1)
	test(8, 7)
	test(64, 63)
	test(64, 64)
	test(1024, 128)
}

func TestEventPoolSlowWait(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	test := func(p pool) {
		const eventSize = 1
		event := p.get(eventSize) // Empty the pool.
		r.Equal(int64(1), p.inUse())

		eventReleased := false

		const waiters = 16
		// Create 16 goroutines to wait on new events.
		wg := new(sync.WaitGroup)
		for range waiters {
			wg.Add(1)
			go func() {
				defer wg.Done()
				e := p.get(eventSize)
				if !eventReleased {
					r.FailNowf("pool have to be empty", "event: %v", e)
				}
				p.back(e)
			}()
		}

		// Wait for all goroutines to be waiting.
		for range 50 {
			if p.waiters() == waiters {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r.Equal(p.waiters(), int64(waiters))
		r.Equal(int64(1), p.inUse())

		// Release events to wakeup waiters.
		eventReleased = true
		p.back(event)

		wg.Wait()

		r.Equal(int64(0), p.waiters())
		r.Equal(int64(0), p.inUse())
	}

	t.Run("eventPool", func(t *testing.T) {
		pool := newTestEventPool(1)
		test(pool)
	})
	t.Run("lowMemory", func(t *testing.T) {
		pool := newTestLowMemoryEventPool(1)
		test(pool)
	})
}

func TestWakeupWaiters(t *testing.T) {
	t.Parallel()
	const (
		poolCapacity = 256
		concurrency  = 5_000
	)
	pool := newTestEventPool(poolCapacity)
	for range 1000 {
		wg := new(sync.WaitGroup)
		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				e := pool.get(1)
				runtime.Gosched()
				pool.back(e)
			}()
		}
		wg.Wait()
	}
}

// testWakeupInterval is used to speed up the test a bit.
const testWakeupInterval = time.Second * 1

func newTestEventPool(capacity int) *eventPool {
	p := newEventPool(capacity, DefaultAvgInputEventSize)
	p.wakeupInterval = testWakeupInterval
	return p
}

func newTestLowMemoryEventPool(capacity int) *lowMemoryEventPool {
	p := newLowMemoryEventPool(capacity)
	p.wakeupInterval = testWakeupInterval
	return p
}
