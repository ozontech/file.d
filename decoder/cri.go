package decoder

import (
	"bytes"
	"fmt"

	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	criDelimiter = ' '
)

// Examples of format:
// 2016-10-06T00:17:09.669794202Z stdout P log content 1
// 2016-10-06T00:17:09.669794203Z stderr F log content
func DecodeCRI(event *insaneJSON.Root, data []byte) error {
	// time
	pos := bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return fmt.Errorf("timestamp is not found")
	}

	time := data[:pos]
	data = data[pos+1:]

	// stream type
	pos = bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return fmt.Errorf("stream type is not found")
	}

	stream := data[:pos]
	data = data[pos+1:]

	// tags
	pos = bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return fmt.Errorf("log tag is not found")
	}

	tags := data[:pos]
	data = data[pos+1:]

	isPartial := tags[0] == 'P'

	// log
	log := data

	// remove \n from log for partial logs
	if isPartial {
		log = log[:len(log)-1]
	}

	event.AddFieldNoAlloc(event, "log").MutateToBytesCopy(event, log)
	event.AddFieldNoAlloc(event, "time").MutateToBytesCopy(event, time)
	event.AddFieldNoAlloc(event, "stream").MutateToBytesCopy(event, stream)

	return nil
}
