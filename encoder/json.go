package encoder

import "github.com/ozontech/file.d/pipeline"

type JSONEncoderParams struct{}

type JSONEncoder struct{}

func newJSONEncoder(_ *JSONEncoderParams) *JSONEncoder {
	return &JSONEncoder{}
}

func (e *JSONEncoder) Encode(event *pipeline.Event, buf []byte) []byte {
	buf, _ = event.Encode(buf)
	return buf
}
