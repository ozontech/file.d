package pipeline


type pipeline struct {
}

func (p *pipeline) start(splitBuffer *splitBuffer) {
	for {
		event := splitBuffer.pop()

		// pipeline logic will be here

		splitBuffer.commit(event)
	}
}
