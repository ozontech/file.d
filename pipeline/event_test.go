package pipeline

import (
	"runtime"
	"sync"
	"testing"
)

func BenchmarkEventPoolEven(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity)

	for i := 0; i < b.N; i++ {
		p.back(p.get())
	}
}

func BenchmarkEventPoolManyGetters(b *testing.B) {
	const capacity = 32

	p := newEventPool(capacity)

	for i := 0; i < b.N; i++ {
		wg := &sync.WaitGroup{}
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func() {
				e := p.get()
				runtime.Gosched()
				p.back(e)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
