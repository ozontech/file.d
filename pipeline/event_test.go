package pipeline

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventPoolDump(t *testing.T) {
	eventPool := newEventPool(2, DefaultAvgInputEventSize)
	e := eventPool.get(1)
	defer eventPool.back(e)

	require.NotPanics(t, func() {
		_ = eventPool.dump()
	})
}

func BenchmarkEventPoolOneGoroutine(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		for i := 0; i < b.N; i++ {
			p.back(p.get(1))
		}
	}
	const capacity = 32
	b.Run("eventPool", func(b *testing.B) {
		p := newEventPool(capacity, DefaultAvgInputEventSize)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func BenchmarkEventPoolManyGoroutines(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		workers := runtime.GOMAXPROCS(0)
		for i := 0; i < b.N; i++ {
			wg := &sync.WaitGroup{}
			wg.Add(workers)
			for j := 0; j < workers; j++ {
				go func() {
					defer wg.Done()

					for k := 0; k < 1000; k++ {
						p.back(p.get(1))
					}
				}()
			}
			wg.Wait()
		}
	}
	const capacity = 32
	b.Run("eventPool", func(b *testing.B) {
		p := newEventPool(capacity, DefaultAvgInputEventSize)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func BenchmarkEventPoolSlowestPath(b *testing.B) {
	bench := func(b *testing.B, p pool) {
		wg := &sync.WaitGroup{}
		for i := 0; i < b.N; i++ {
			const concurrency = 1_000
			wg.Add(concurrency)
			for j := 0; j < concurrency; j++ {
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
		p := newEventPool(capacity, DefaultAvgInputEventSize)
		bench(b, p)
	})
	b.Run("lowMemory", func(b *testing.B) {
		p := newLowMemoryEventPool(capacity)
		bench(b, p)
	})
}

func TestLowMemPool(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	test := func(capacity, batchSize int) {
		p := newLowMemoryEventPool(capacity)
		for i := 0; i < batchSize; i++ {
			batch := make([]*Event, batchSize)
			for j := 0; j < batchSize; j++ {
				batch[j] = p.get(1)
			}
			r.Equal(int64(batchSize), p.inUse())
			r.Equal(int64(0), p.waiters())

			for j := 0; j < batchSize; j++ {
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

func TestLowMemPoolSlowWait(t *testing.T) {
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
		for i := 0; i < waiters; i++ {
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
		for i := 0; i < 50; i++ {
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

	t.Run("lowMemory", func(t *testing.T) {
		pool := newLowMemoryEventPool(1)
		test(pool)
	})
	// TODO: add test for eventPool after #685.
}
