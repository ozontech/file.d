package encoder

import "github.com/ozontech/file.d/pipeline"

type RawEncoderParams struct {
	Field string `json:"field" default:"message"`
}

type RawEncoder struct {
	field string
}

func newRawEncoder(params *RawEncoderParams) *RawEncoder {
	field := params.Field
	if field == "" {
		field = "message"
	}
	return &RawEncoder{field: field}
}

func (e *RawEncoder) Encode(event *pipeline.Event, buf []byte) []byte {
	node := event.Root.Dig(e.field)
	if node == nil {
		return buf[:0]
	}

	if node.IsString() {
		return append(buf, node.AsBytes()...)
	}

	return node.Encode(buf)
}
