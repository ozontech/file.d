package pipeline

import (
	"gitlab.ozon.ru/sre/filed/logger"
)

type Head struct {
	splitBuffer *splitBuffer
}

func NewHead(splitBuffer *splitBuffer) *Head {
	return &Head{
		splitBuffer: splitBuffer,
	}
}

func (i *Head) Push(source InputPlugin, sourceId uint64, from int64, delta int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	event := i.splitBuffer.Reserve()
	parser := event.parser
	json, err := parser.ParseBytes(bytes)
	if err != nil {
		logger.Fatalf("wrong json %s at offset %d, prev line: %s", string(bytes), from, i.splitBuffer.events[i.splitBuffer.eventsCount-1].raw)
		return
	}

	event.raw = bytes
	event.json = json
	event.input = source
	event.Offset = from + delta
	event.SourceId = sourceId
	if json.Get("stream") != nil {
		event.Stream = json.Get("stream").String()
	} else {
		event.Stream = "default"
	}

	i.splitBuffer.Push(event)
}
