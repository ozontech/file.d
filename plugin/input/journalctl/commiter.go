package journalctl

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ozontech/file.d/pipeline"
)

type SaveOffsetsFunc func(info offsetInfo)

type Commiter interface {
	Commit(event *pipeline.Event)
	Shutdown()
}

type SyncCommiter struct {
	offset offsetInfo
	save   SaveOffsetsFunc
}

func NewSyncCommiter(save SaveOffsetsFunc) *SyncCommiter {
	return &SyncCommiter{save: save}
}

var _ Commiter = &SyncCommiter{}

func (a *SyncCommiter) Commit(event *pipeline.Event) {
	a.offset.set(strings.Clone(event.Root.Dig("__CURSOR").AsString()))
	a.save(a.offset)
}

func (a *SyncCommiter) Shutdown() {
	// do nothing because we are saved the offsets in the commit func
}

type AsyncCommiter struct {
	mu sync.Mutex

	offset    atomic.Pointer[offsetInfo]
	debouncer Debouncer
	save      SaveOffsetsFunc
}

func NewAsyncCommiter(debouncer Debouncer, save SaveOffsetsFunc) *AsyncCommiter {
	commiter := &AsyncCommiter{debouncer: debouncer, save: save}
	commiter.offset.Store(&offsetInfo{})
	return commiter
}

var _ Commiter = &AsyncCommiter{}

func (a *AsyncCommiter) Commit(event *pipeline.Event) {
	offInfo := *a.offset.Load()
	offInfo.set(strings.Clone(event.Root.Dig("__CURSOR").AsString()))
	a.offset.Store(&offInfo)

	// save offsets
	a.mu.Lock()
	defer a.mu.Unlock()

	a.debouncer.Do(func() {
		a.save(offInfo)
	})
}

func (a *AsyncCommiter) Shutdown() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.debouncer.Do(func() {
		a.save(*a.offset.Load())
	})
}
