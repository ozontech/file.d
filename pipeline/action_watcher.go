package pipeline

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// actionWatcher is the struct for debugging actions.
// It creates one sample for each `/sample` endpoint call.
type actionWatcher struct {
	procID int

	// samples is the map of actionIndex to slice of not-ready-yet samples for this action.
	samples map[int][]*sample
	// samplesLen is needed for not locking the processor in case of nobody wait the samples.
	samplesLen *atomic.Int64
	samplesMu  sync.Mutex
}

// sample is debug info of the given processor and the action.
// It holds an event before the action processing and what it's become after the processing.
type sample struct {
	procID      int
	readyCh     chan bool
	eventBefore []byte
	eventAfter  []byte
	eventStatus eventStatus
}

var errTimeout = errors.New("timeout while wait an action sample")

// newActionWatcher creates actionWatcher for the given processor.
func newActionWatcher(procID int) *actionWatcher {
	return &actionWatcher{
		procID:     procID,
		samples:    make(map[int][]*sample),
		samplesLen: atomic.NewInt64(0),
		samplesMu:  sync.Mutex{},
	}
}

// watch adds a sample and waits until the processor fill it.
// Every processor has its actionWatcher. When the watcher adds the sample,
// the process tries to fill it (via setEventBefore and setEventAfter methods),
// and then send `true` to `readyCh`. The watcher then returns the sample
// and deletes it from waiting samples.
func (aw *actionWatcher) watch(actionIdx int, timeout time.Duration) (*sample, error) {
	s := aw.addSample(actionIdx)
	defer aw.deleteSample(actionIdx, s)

	select {
	case <-s.ready():
		return s, nil
	case <-time.After(timeout):
		return nil, errTimeout
	}
}

func (aw *actionWatcher) addSample(actionIdx int) *sample {
	s := &sample{
		procID:      aw.procID,
		readyCh:     make(chan bool, 1),
		eventBefore: nil,
		eventAfter:  nil,
		eventStatus: eventStatus(""),
	}
	aw.samplesMu.Lock()
	aw.samples[actionIdx] = append(aw.samples[actionIdx], s)
	aw.samplesLen.Inc()
	aw.samplesMu.Unlock()

	return s
}

func (aw *actionWatcher) deleteSample(actionIdx int, sample *sample) {
	aw.samplesMu.Lock()
	defer aw.samplesMu.Unlock()

	samples := aw.samples[actionIdx]

	deleteInd := 0
	for i, s := range samples {
		if sample == s {
			deleteInd = i

			break
		}
	}

	samples[deleteInd] = samples[len(samples)-1]
	samples[len(samples)-1] = nil
	aw.samples[actionIdx] = samples[:len(samples)-1]
	aw.samplesLen.Dec()
}

// setEventBefore sets events before for every sample.
func (aw *actionWatcher) setEventBefore(index int, event *Event) {
	if aw.samplesLen.Load() <= 0 {
		return
	}

	aw.samplesMu.Lock()
	defer aw.samplesMu.Unlock()

	for _, s := range aw.samples[index] {
		if s == nil || len(s.eventBefore) > 0 {
			continue
		}
		s.eventBefore = event.Root.EncodeToByte()
	}
}

// setEventAfter sets eventAfter for every sample of the given action with index
// and signal `watch` func that the sample is ready.
func (aw *actionWatcher) setEventAfter(index int, event *Event, result eventStatus) {
	if aw.samplesLen.Load() <= 0 {
		return
	}

	aw.samplesMu.Lock()
	defer aw.samplesMu.Unlock()

	for _, s := range aw.samples[index] {
		if s == nil || len(s.eventAfter) > 0 || len(s.eventBefore) == 0 {
			return
		}

		s.eventAfter = event.Root.EncodeToByte()
		s.eventStatus = result
		s.readyCh <- true
	}
}

func (s *sample) ready() <-chan bool {
	return s.readyCh
}

func (s *sample) Marshal() []byte {
	type Result struct {
		ProcessorID int            `json:"processor_id"`
		EventBefore map[string]any `json:"event_before"`
		EventAfter  map[string]any `json:"event_after"`
		EventStatus string         `json:"event_status"`
	}
	r := Result{
		ProcessorID: s.procID,
		EventBefore: map[string]any{},
		EventAfter:  map[string]any{},
		EventStatus: string(s.eventStatus),
	}
	_ = json.Unmarshal(s.eventBefore, &r.EventBefore)
	_ = json.Unmarshal(s.eventAfter, &r.EventAfter)
	resp, _ := json.Marshal(r)

	return resp
}
