package pipeline

type Parser struct {
	splitBuffer *splitBuffer
}

func (i *Parser) Put(bytes []byte) {
	event := i.splitBuffer.reserve()

	jsonParser := event.parser
	json, err := jsonParser.ParseBytes(bytes)
	if err != nil {
		panic("wrong json:" + string(bytes))
	}
	event.raw = bytes
	event.json = json

	i.splitBuffer.push(event)
}
