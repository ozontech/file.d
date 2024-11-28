package decoder

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/tidwall/gjson"
)

const (
	jsonMaxFieldsSizeParam = "json_max_fields_size"
)

type jsonParams struct {
	maxFieldsSize map[string]int // optional
}

type jsonCutPos struct {
	start int
	end   int
}

type jsonDecoder struct {
	params jsonParams

	cutPositions []jsonCutPos
	mu           *sync.Mutex
}

func NewJsonDecoder(params map[string]any) (Decoder, error) {
	p, err := extractJsonParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	if len(p.maxFieldsSize) < 2 {
		return &jsonDecoder{params: p}, nil
	}

	return &jsonDecoder{
		params: p,

		cutPositions: make([]jsonCutPos, 0, len(p.maxFieldsSize)),
		mu:           &sync.Mutex{},
	}, nil
}

func (d *jsonDecoder) Type() Type {
	return JSON
}

// DecodeToJson decodes json-formatted string and merges result with root.
func (d *jsonDecoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	data = d.cutFieldsBySize(data)
	return root.DecodeBytes(data)
}

// Decode decodes json-formatted string to [*insaneJSON.Node].
//
// Args:
//   - root [*insaneJSON.Root] - required
func (d *jsonDecoder) Decode(data []byte, args ...any) (any, error) {
	if len(args) == 0 {
		return nil, errors.New("empty args")
	}
	root, ok := args[0].(*insaneJSON.Root)
	if !ok {
		return nil, errors.New("invalid args")
	}
	data = d.cutFieldsBySize(data)
	return root.DecodeBytesAdditional(data)
}

func (d *jsonDecoder) cutFieldsBySize(data []byte) []byte {
	if len(d.params.maxFieldsSize) == 0 || !gjson.ValidBytes(data) {
		return data
	}

	findPos := func(path string, limit int) (jsonCutPos, bool) {
		if path == "" {
			return jsonCutPos{}, false
		}

		v := gjson.GetBytes(data, path)
		if !v.Exists() || v.Type != gjson.String || len(v.Str) <= limit {
			return jsonCutPos{}, false
		}

		// [v.Index] is value start position including quote (")
		return jsonCutPos{
			start: v.Index + limit + 1,
			end:   v.Index + len(v.Str),
		}, true
	}

	// fast way
	if len(d.params.maxFieldsSize) == 1 {
		var (
			pos jsonCutPos
			ok  bool
		)
		for path, limit := range d.params.maxFieldsSize {
			pos, ok = findPos(path, limit)
		}
		if !ok {
			return data
		}
		return append(data[:pos.start], data[pos.end+1:]...)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.cutPositions = d.cutPositions[:0]
	for path, limit := range d.params.maxFieldsSize {
		if pos, ok := findPos(path, limit); ok {
			d.cutPositions = append(d.cutPositions, pos)
		}
	}

	// sort by desc
	slices.SortFunc(d.cutPositions, func(p1, p2 jsonCutPos) int {
		return p2.start - p1.start
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
			var vInt int

			switch vNum := v.(type) {
			case int:
				vInt = vNum
			case float64:
				vInt = int(vNum)
			case json.Number:
				vInt64, err := vNum.Int64()
				if err != nil {
					return jsonParams{}, fmt.Errorf("each value in %q must be int", jsonMaxFieldsSizeParam)
				}
				vInt = int(vInt64)
			default:
				return jsonParams{}, fmt.Errorf("each value in %q must be int", jsonMaxFieldsSizeParam)
			}

			maxFieldsSize[k] = vInt
		}
	}

	return jsonParams{
		maxFieldsSize: maxFieldsSize,
	}, nil
}
