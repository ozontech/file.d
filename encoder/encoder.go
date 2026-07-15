package encoder

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
	Encode(event *pipeline.Event, buf []byte) ([]byte, error)
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
