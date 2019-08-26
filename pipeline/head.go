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

func (i *Head) Push(acceptor InputPluginAcceptor, sourceId uint64, additional string, from int64, delta int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	event := i.splitBuffer.Reserve()
	json, err := event.parser.ParseBytes(bytes)
	if err != nil {
		logger.Fatalf("wrong json %s at offset %d", string(bytes), from)
		return
	}

	stream := "default"
	if json.Get("stream") != nil {
		stream = json.Get("stream").String()
	}

	event.raw = bytes
	event.JSON = json
	event.acceptor = acceptor

	event.Offset = from + delta
	event.SourceId = sourceId
	event.Stream = stream
	event.Additional = additional

	i.splitBuffer.Push(event)
}
