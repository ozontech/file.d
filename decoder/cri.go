package decoder

import (
	"bytes"
	"fmt"
)

const (
	criDelimiter = ' '
)

type CRIRow struct {
	Log, Time, Stream []byte
	IsPartial         bool
}

// DecodeCRI decodes CRI formatted event.
// Examples of format:
// 2016-10-06T00:17:09.669794202Z stdout P log content 1
// 2016-10-06T00:17:09.669794203Z stderr F log content
func DecodeCRI(data []byte) (row CRIRow, _ error) {
	// time
	pos := bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("timestamp is not found")
	}

	row.Time = data[:pos]
	data = data[pos+1:]

	// stream type
	pos = bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("stream type is not found")
	}

	stream := data[:pos]
	// stderr or stdout
	if len(stream) != 6 {
		return row, fmt.Errorf("stream is unknown")
	}
	row.Stream = stream

	data = data[pos+1:]

	// tags
	pos = bytes.IndexByte(data, criDelimiter)
	if pos < 0 {
		return row, fmt.Errorf("log tag is not found")
	}

	tags := data[:pos]
	data = data[pos+1:]

	if len(tags) == 0 {
		return row, fmt.Errorf("log tag is empty")
	}

	row.IsPartial = tags[0] == 'P'

	log := data
	// remove \n from log for partial logs
	if row.IsPartial {
		log = log[:len(log)-1]
	}

	row.Log = log

	return row, nil
}
