package pipeline

type pipeline struct {
	controller *SplitController

	streams    []*stream
	nextStream chan *stream

	shouldExit bool
}

func NewPipeline(controller *SplitController) *pipeline {
	return &pipeline{
		controller: controller,
		streams:    make([]*stream, 0, 16),
		nextStream: make(chan *stream, controller.capacity),
	}
}

func (p *pipeline) addStream(stream *stream) {
	p.streams = append(p.streams, stream)
	stream.pipeline = p
}

func (p *pipeline) start() {
	go p.process()
}

func (p *pipeline) stop() {
	p.shouldExit = true
	// unblock process function to allow goroutine to exit
	p.nextStream <- nil
}

func (p *pipeline) process() {
	for {
		event := p.getEvent()
		if p.shouldExit {
			return
		}

		//pipeline logic will be here

		p.controller.commit(event)
	}
}

func (p *pipeline) getEvent() *Event {
	for {
		stream := <-p.nextStream
		if p.shouldExit {
			return nil
		}
		event := stream.tryPop()
		if event != nil {
			return event
		}
	}
}
