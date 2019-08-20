package pipeline

type Parser struct {
	splitBuffer *splitBuffer
}

func NewParser(splitBuffer *splitBuffer) *Parser {
	return &Parser{
		splitBuffer: splitBuffer,
	}
}

func (i *Parser) Put(source InputPlugin, sourceId uint64, offset int64, bytes []byte) {
	event := i.splitBuffer.Reserve()

	jsonParser := event.parser
	json, err := jsonParser.ParseBytes(bytes)
	if err != nil {
		panic("wrong json:" + string(bytes))
	}

	event.raw = bytes
	event.json = json
	event.input = source
	event.Offset = offset
	event.SourceId = sourceId
	if json.Get("sourceId") != nil {
		event.Stream = json.Get("sourceId").String()
	} else {
		event.Stream = "default"
	}

	i.splitBuffer.Push(event)
}
