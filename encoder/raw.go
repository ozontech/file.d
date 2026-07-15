package encoder

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline"
)

const defaultRawField = "message"

var ErrFieldNotFound = errors.New("field not found")

type RawEncoderParams struct {
	Field string `json:"field"`
}

type RawEncoder struct {
	field string
}

func newRawEncoder(params *RawEncoderParams) *RawEncoder {
	field := params.Field
	if field == "" {
		field = defaultRawField
	}
	return &RawEncoder{field: field}
}

func (e *RawEncoder) Encode(event *pipeline.Event, buf []byte) ([]byte, error) {
	node := event.Root.Dig(e.field)
	if node == nil {
		return buf, fmt.Errorf("%w: %q", ErrFieldNotFound, e.field)
	}

	if node.IsString() {
		return append(buf, node.AsBytes()...), nil
	}

	return node.Encode(buf), nil
}
