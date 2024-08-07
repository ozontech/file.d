package decoder

import (
	"bytes"
	"fmt"
	"strings"
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

	var stream []byte
	// stderr or stdout
	for len(stream) != 6 {
		// stream type
		pos = bytes.IndexByte(data, criDelimiter)
		if pos < 0 {
			return row, fmt.Errorf("stream type is not found")
		}
		stream = data[:pos]
		data = data[pos+1:]
	}
	row.Stream = stream

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

type CRIMetaInformation struct {
	namespace     string
	podName       string
	containerName string
	containerID   string
}

func NewCRIMetaInformation(fullFilename string) CRIMetaInformation {
	lastSlash := strings.LastIndexByte(fullFilename, '/')
	filename := fullFilename[lastSlash+1 : len(fullFilename)-4]
	underscore := strings.IndexByte(filename, '_')
	pod := filename[:underscore]
	filename = filename[underscore+1:]
	underscore = strings.IndexByte(filename, '_')
	ns := filename[:underscore]
	filename = filename[underscore+1:]
	container := filename[:len(filename)-65]
	cid := filename[len(filename)-64:]

	return CRIMetaInformation{
		ns, pod, container, cid,
	}
}

func (m CRIMetaInformation) GetData() map[string]any {
	return map[string]any{
		"pod":          m.podName,
		"namespace":    m.namespace,
		"container":    m.containerName,
		"container_id": m.containerID,
	}
}
