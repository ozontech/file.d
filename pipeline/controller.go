package pipeline

type Controller struct {
	Parsers     []*Parser
	Pipelines   []*pipeline
	SplitBuffer *splitBuffer
}

func NewController(isTest bool) *Controller {
	splitBuffer := newSplitBuffer(1, isTest)

	pipelineCount := 1
	parsersCount := 1

	pipelines := make([]*pipeline, pipelineCount)
	for i := 0; i < pipelineCount; i++ {
		pipelines[i] = &pipeline{}
		go pipelines[i].start(splitBuffer)
	}

	parsers := make([]*Parser, parsersCount)
	for i := 0; i < parsersCount; i++ {
		parsers[i] = &Parser{
			splitBuffer: splitBuffer,
		}
	}

	return &Controller{
		Pipelines:   pipelines,
		Parsers:     parsers,
		SplitBuffer: splitBuffer,
	}
}
