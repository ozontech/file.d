package http

import (
	"encoding/json"
	"fmt"

	"github.com/ozontech/file.d/pipeline"
)

const (
	EncoderTypeJSON = "json"
	EncoderTypeRaw  = "raw"
)

type Encoder interface {
	Encode(event *pipeline.Event, buf []byte) []byte
}

type JSONEncoderParams struct{}

type JSONEncoder struct{}

func newJSONEncoder(_ *JSONEncoderParams) *JSONEncoder {
	return &JSONEncoder{}
}

func (e *JSONEncoder) Encode(event *pipeline.Event, buf []byte) []byte {
	buf, _ = event.Encode(buf)
	return buf
}

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
		return []byte{}
	}
	return append(buf, node.EncodeToByte()...)
}

type EncodingConfig struct {
	Type   string          `json:"type" default:"json" options:"json|raw"`
	Params json.RawMessage `json:"params"`
}

func NewEncoder(cfg EncodingConfig) (Encoder, error) {
	switch cfg.Type {
	case EncoderTypeJSON, "":
		return newJSONEncoder(&JSONEncoderParams{}), nil

	case EncoderTypeRaw:
		var params RawEncoderParams
		if len(cfg.Params) > 0 {
			if err := json.Unmarshal(cfg.Params, &params); err != nil {
				return nil, fmt.Errorf("raw encoder params: %w", err)
			}
		}
		return newRawEncoder(&params), nil

	default:
		return nil, fmt.Errorf("unknown encoding type %q; supported: json, raw", cfg.Type)
	}
}
