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

func newJSONEncoder(_ *JSONEncoderParams) (*JSONEncoder, error) {
	return &JSONEncoder{}, nil
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

func newRawEncoder(params *RawEncoderParams) (*RawEncoder, error) {
	field := params.Field
	if field == "" {
		field = "message"
	}
	return &RawEncoder{field: field}, nil
}

func (e *RawEncoder) Encode(event *pipeline.Event, buf []byte) []byte {
	node := event.Root.Dig(e.field)
	if node == nil {
		return []byte{}
	}
	return append(buf, node.EncodeToByte()...)
}

type EncodingConfig struct {
	// > @3@4@5@6
	// >
	// > Codec to use for serialising events:
	// > * `json` — serialises the full event as a JSON object (default).
	// > * `raw`  — extracts a single field and sends its value as-is.
	Type string `json:"type" default:"json" options:"json|raw"` // *

	// > @3@4@5@6
	// >
	// > Encoder parameters.
	Params json.RawMessage `json:"params"` // *
}

func NewEncoder(cfg EncodingConfig) (Encoder, error) {
	switch cfg.Type {
	case EncoderTypeJSON, "":
		return newJSONEncoder(&JSONEncoderParams{})

	case EncoderTypeRaw:
		var params RawEncoderParams
		if len(cfg.Params) > 0 {
			if err := json.Unmarshal(cfg.Params, &params); err != nil {
				return nil, fmt.Errorf("raw encoder params: %w", err)
			}
		}
		return newRawEncoder(&params)

	default:
		return nil, fmt.Errorf("unknown encoding type %q; supported: json, raw", cfg.Type)
	}
}
