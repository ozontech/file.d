package pipeline

type Parser struct {
	splitBuffer *splitBuffer
}

func NewParser(splitBuffer *splitBuffer) *Parser {
	return &Parser{
		splitBuffer: splitBuffer,
	}
}

func (i *Parser) Put(source InputPlugin, stream string, offset int64, bytes []byte) {
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
	event.Stream = stream
	if json.Get("stream") != nil {
		event.SubStream = json.Get("stream").String()
	} else {
		event.SubStream = "default"
	}

	i.splitBuffer.Push(event)
}
