package pipeline

// track is worker goroutine which doing pipeline actions
type Track struct {
	pipeline     *SplitPipeline
	descriptions []*PluginDescription
	actions      []ActionPlugin

	streams    []*stream
	nextStream chan *stream

	stopCh chan bool
}

func NewTrack(pipeline *SplitPipeline) *Track {
	return &Track{
		pipeline:   pipeline,
		streams:    make([]*stream, 0, 16),
		nextStream: make(chan *stream, pipeline.capacity),
		stopCh:     make(chan bool),
	}
}

func (t *Track) addStream(stream *stream) {
	t.streams = append(t.streams, stream)
	stream.track = t
}

func (t *Track) start() {
	for _, pd := range t.descriptions {
		pd.Plugin.(ActionPlugin).Start(pd.Config, t)
	}

	go t.process()
}

func (t *Track) stop() {
	for _, action := range t.actions {
		action.Stop()
	}

	t.stopCh <- true
}

func (t *Track) process() {
	for {
		event := t.getEvent()
		if event == nil {
			return
		}

		for _, action := range t.actions {
			action.Do(event)
		}

		t.pipeline.commit(event)
	}
}

func (t *Track) getEvent() *Event {
	for {
		select {
		case stream := <-t.nextStream:
			event := stream.tryPop()
			if event != nil {
				return event
			}
		case <-t.stopCh:
			return nil
		}
	}
}

func (t *Track) AddActionPlugin(plugin *PluginDescription) {
	t.descriptions = append(t.descriptions, plugin)
	t.actions = append(t.actions, plugin.Plugin.(ActionPlugin))
}
