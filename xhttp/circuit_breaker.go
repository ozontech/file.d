package xhttp

import (
	"context"
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"
)

type TargetID string

type Target[T any] struct {
	ID     TargetID
	Client T
	Weight int
}

type CircuitBreaker[T any] struct {
	activeTargets []Target[T]
	targetsByID   map[TargetID]T
	weightsByID   map[TargetID]int
	bannedUntil   map[TargetID]time.Time
	banPeriod     time.Duration
	mu            sync.RWMutex
}

func NewCircuitBreaker[T any](banPeriod time.Duration, activeTargetsCap int) *CircuitBreaker[T] {
	return &CircuitBreaker[T]{
		activeTargets: make([]Target[T], 0, activeTargetsCap),
		targetsByID:   make(map[TargetID]T, activeTargetsCap),
		weightsByID:   make(map[TargetID]int, activeTargetsCap),
		bannedUntil:   make(map[TargetID]time.Time, activeTargetsCap),
		banPeriod:     banPeriod,
	}
}

func (cb *CircuitBreaker[T]) AddTarget(id TargetID, client T, weight int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.targetsByID[id] = client
	cb.weightsByID[id] = weight

	for i := 0; i < weight; i++ {
		cb.activeTargets = append(cb.activeTargets, Target[T]{
			ID:     id,
			Client: client,
			Weight: weight,
		})
	}
}

func (cb *CircuitBreaker[T]) BanTarget(id TargetID) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	filtered := cb.activeTargets[:0]
	for _, target := range cb.activeTargets {
		if target.ID != id {
			filtered = append(filtered, target)
		}
	}

	cb.activeTargets = filtered
	cb.bannedUntil[id] = xtime.GetInaccurateTime().Add(cb.banPeriod)
}

func (cb *CircuitBreaker[T]) RestoreBannedTargets() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	for id, until := range cb.bannedUntil {
		if xtime.GetInaccurateTime().Before(until) {
			continue
		}

		client := cb.targetsByID[id]
		weight := cb.weightsByID[id]

		for i := 0; i < weight; i++ {
			cb.activeTargets = append(cb.activeTargets, Target[T]{
				ID:     id,
				Client: client,
				Weight: weight,
			})
		}

		delete(cb.bannedUntil, id)
	}
}

func (cb *CircuitBreaker[T]) CalcActiveTargetsCapacity(target []T, getWeight func(T) int) int {
	totalCap := 0
	for _, t := range target {
		w := getWeight(t)
		totalCap += w
	}
	return totalCap
}

func CheckBannedHosts[T any](ctx context.Context, cb *CircuitBreaker[T], reconnectInterval time.Duration) {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cb.RestoreBannedTargets()
		}
	}
}
