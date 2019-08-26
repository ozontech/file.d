package pipeline

// track is worker goroutine which doing pipeline actions
type Track struct {
	pipeline     *SplitPipeline
	descriptions []*PluginDescription
	actions      []ActionPlugin

	shouldStop bool

	streams    []*stream
	nextStream chan *stream
}

func NewTrack(pipeline *SplitPipeline) *Track {
	return &Track{
		pipeline:   pipeline,
		streams:    make([]*stream, 0, 16),
		nextStream: make(chan *stream, pipeline.capacity),
	}
}

func (t *Track) addStream(stream *stream) {
	t.streams = append(t.streams, stream)
	stream.track = t
}

func (t *Track) start() {
	t.shouldStop = false
	for _, pd := range t.descriptions {
		pd.Plugin.(ActionPlugin).Start(pd.Config, t)
	}

	go t.process()
}

func (t *Track) stop() {
	for _, action := range t.actions {
		action.Stop()
	}

	t.shouldStop = true
	//unblock goroutine
	t.nextStream <- nil
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
		stream := <-t.nextStream
		if t.shouldStop {
			return nil
		}
		event := stream.tryPop()
		if event != nil {
			return event
		}
	}
}

func (t *Track) AddActionPlugin(plugin *PluginDescription) {
	t.descriptions = append(t.descriptions, plugin)
	t.actions = append(t.actions, plugin.Plugin.(ActionPlugin))
}
