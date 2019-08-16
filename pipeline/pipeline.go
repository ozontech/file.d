package pipeline

import (
	"time"
)

type pipeline struct {
	controller  *Controller
	streams     []*stream
	nextStream  chan *stream
	splitBuffer *splitBuffer
	ticker      *time.Ticker
	shouldExit  bool
}

func NewPipeline(controller *Controller) *pipeline {
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

func (p *pipeline) start(splitBuffer *splitBuffer) {
	p.splitBuffer = splitBuffer
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
		p.splitBuffer.commit(event)
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
