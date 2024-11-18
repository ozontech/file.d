package decoder

import (
	"errors"
	"fmt"
	"slices"

	"github.com/tidwall/gjson"
	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	jsonMaxFieldsSizeParam = "json_max_fields_size"
)

type jsonParams struct {
	MaxFieldsSize map[string]int // optional
}

type jsonCutPos struct {
	start int
	end   int
}

type JsonDecoder struct {
	params       jsonParams
	cutPositions []jsonCutPos
}

func NewJsonDecoder(params map[string]any) (*JsonDecoder, error) {
	p, err := extractJsonParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &JsonDecoder{
		params:       p,
		cutPositions: make([]jsonCutPos, 0, len(p.MaxFieldsSize)),
	}, nil
}

func (d *JsonDecoder) Type() Type {
	return JSON
}

// DecodeToJson decodes json-formatted string and merges result with root.
func (d *JsonDecoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	data = d.checkFieldsSize(data)
	return root.DecodeBytes(data)
}

// Decode decodes json-formatted string to [*insaneJSON.Node].
//
// Args:
//   - root [*insaneJSON.Root] - required
func (d *JsonDecoder) Decode(data []byte, args ...any) (any, error) {
	if len(args) == 0 {
		return nil, errors.New("empty args")
	}
	root, ok := args[0].(*insaneJSON.Root)
	if !ok {
		return nil, errors.New("invalid args")
	}
	data = d.checkFieldsSize(data)
	return root.DecodeBytesAdditional(data)
}

func (d *JsonDecoder) checkFieldsSize(data []byte) []byte {
	d.cutPositions = d.cutPositions[:0]
	if !gjson.ValidBytes(data) {
		return data
	}

	for path, limit := range d.params.MaxFieldsSize {
		if path == "" {
			continue
		}

		v := gjson.GetBytes(data, path)
		if !v.Exists() || v.Type != gjson.String || len(v.Str) <= limit {
			continue
		}

		// [v.Index] is value start position including quote (")
		d.cutPositions = append(d.cutPositions, jsonCutPos{
			start: v.Index + limit + 1,
			end:   v.Index + len(v.Str),
		})
	}

	// sort by desc
	slices.SortFunc(d.cutPositions, func(p1, p2 jsonCutPos) int {
		if p1.start > p2.start {
			return -1
		} else if p1.start < p2.start {
			return 1
		}
		return 0
	})
	for _, p := range d.cutPositions {
		data = append(data[:p.start], data[p.end+1:]...)
	}

	return data
}

func extractJsonParams(params map[string]any) (jsonParams, error) {
	maxFieldsSize := make(map[string]int)
	if maxFieldsSizeRaw, ok := params[jsonMaxFieldsSizeParam]; ok {
		maxFieldsSizeMap, ok := maxFieldsSizeRaw.(map[string]any)
		if !ok {
			return jsonParams{}, fmt.Errorf("%q must be map", jsonMaxFieldsSizeParam)
		}
		for k, v := range maxFieldsSizeMap {
			vInt, ok := v.(int)
			if !ok {
				return jsonParams{}, fmt.Errorf("each value in %q must be int", jsonMaxFieldsSizeParam)
			}
			maxFieldsSize[k] = vInt
		}
	}

	return jsonParams{
		MaxFieldsSize: maxFieldsSize,
	}, nil
}
