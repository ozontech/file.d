package decoder

import (
	"bytes"
	"errors"
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"
)

const (
	columnNamesParam = "column_names"
	prefixParam      = "prefix"
	delimiterParam   = "delimiter"

	defaultPrefix    = ""
	defaultDelimiter = byte(',')
)

type CSVRow []string

type CSVParams struct {
	columnNames []string
	prefix      string
	delimiter   byte
}

type CSVDecoder struct {
	params    CSVParams
	fieldsNum int

	recordBuffer []byte
	fieldIndexes []int
	resultBuffer CSVRow
}

func NewCSVDecoder(params map[string]any) (Decoder, error) {
	p, err := extractCSVParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &CSVDecoder{
		params:    p,
		fieldsNum: len(p.columnNames),
	}, nil
}

func (d *CSVDecoder) Type() Type {
	return CSV
}

// DecodeToJson decodes csv formatted log and merges result with root.
//
// From (column_names: ['timestamp', 'ip', 'service', 'info'], delimiter: ";"):
//
// 1760551019001;127.0.0.1;example-service;some-additional-info
//
// To:
//
//	{
//		"timestamp": "1760551019001",
//		"ip": "127.0.0.1",
//		"service": "example-service",
//		"info": "some-additional-info",
//	}
//
// From (column_names: [], prefix: 'csv_', delimiter: ","):
//
// 1760551019001,127.0.0.1,example-service,some-additional-info
//
// To:
//
//	{
//		"csv_0": "1760551019001",
//		"csv_1": "127.0.0.1",
//		"csv_2": "example-service",
//		"csv_3": "some-additional-info",
//	}
func (d *CSVDecoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	rowRaw, err := d.Decode(data)
	if err != nil {
		return err
	}
	row := rowRaw.(CSVRow)

	if d.fieldsNum != 0 {
		for i := range row {
			root.AddFieldNoAlloc(root, d.params.columnNames[i]).MutateToString(row[i])
		}
	} else {
		for i := range row {
			root.AddFieldNoAlloc(root, fmt.Sprintf("%s%d", d.params.prefix, i)).MutateToString(row[i])
		}
	}

	return nil
}

// Decode decodes CSV formatted log to []string.
//
// Example of format:
//
// "1760551019001	127.0.0.1	example-service	some-additional-info"
func (d *CSVDecoder) Decode(data []byte, _ ...any) (any, error) {
	if !validDelim(d.params.delimiter) {
		return nil, fmt.Errorf("invalid delimiter")
	}

	if n := len(data); n >= 2 && data[n-2] == '\r' && data[n-1] == '\n' {
		data[n-2] = '\n'
		data = data[:n-1]
	}

	var err error
	const quoteLen = 1
	const delimiterLen = 1

	d.recordBuffer = d.recordBuffer[:0]
	d.fieldIndexes = d.fieldIndexes[:0]
parseField:
	for {
		if len(data) == 0 || data[0] != '"' {
			// Non-quoted string field
			i := bytes.IndexByte(data, d.params.delimiter)
			field := data
			if i >= 0 {
				field = field[:i]
			} else {
				field = field[:len(field)-lengthNL(field)]
			}

			if j := bytes.IndexByte(field, '"'); j >= 0 {
				return nil, errors.New("missing \" in non-quoted field")
			}

			d.recordBuffer = append(d.recordBuffer, field...)
			d.fieldIndexes = append(d.fieldIndexes, len(d.recordBuffer))
			if i >= 0 {
				data = data[i+delimiterLen:]
				continue parseField
			}
			break parseField
		} else {
			// Quoted string field
			data = data[quoteLen:]
			for {
				i := bytes.IndexByte(data, '"')
				if i >= 0 {
					// Hit next quote.
					d.recordBuffer = append(d.recordBuffer, data[:i]...)
					data = data[i+quoteLen:]
					switch rn := data[0]; {
					case rn == '"':
						// `""` sequence (append quote).
						d.recordBuffer = append(d.recordBuffer, '"')
						data = data[quoteLen:]
					case rn == d.params.delimiter:
						// `",` sequence (end of field).
						data = data[delimiterLen:]
						d.fieldIndexes = append(d.fieldIndexes, len(d.recordBuffer))
						continue parseField
					case lengthNL(data) == len(data):
						// `"\n` sequence (end of data).
						d.fieldIndexes = append(d.fieldIndexes, len(d.recordBuffer))
						break parseField
					default:
						// `"*` sequence (invalid non-escaped quote).
						return nil, errors.New("invalid non-escaped quote")
					}
				} else {
					return nil, errors.New("missing \" in quoted field")
				}
			}
		}
	}

	str := string(d.recordBuffer)
	d.resultBuffer = d.resultBuffer[:0]
	if cap(d.resultBuffer) < len(d.fieldIndexes) {
		d.resultBuffer = make([]string, len(d.fieldIndexes))
	}
	d.resultBuffer = d.resultBuffer[:len(d.fieldIndexes)]
	var preIdx int
	for i, idx := range d.fieldIndexes {
		d.resultBuffer[i] = str[preIdx:idx]
		preIdx = idx
	}

	if d.fieldsNum > 0 {
		if len(d.resultBuffer) != d.fieldsNum && err == nil {
			err = errors.New("wrong number of fields")
		}
	}

	return d.resultBuffer, err

}

func extractCSVParams(params map[string]any) (CSVParams, error) {
	columnNames := make([]string, 0)
	if columnNamesRaw, ok := params[columnNamesParam]; ok {
		columnNamesRawSlice, ok := columnNamesRaw.([]any)
		if !ok {
			return CSVParams{}, fmt.Errorf("%v must be slice", columnNamesParam)
		}
		for _, v := range columnNamesRawSlice {
			str, ok := v.(string)
			if !ok {
				return CSVParams{}, fmt.Errorf("each value in %q must be string", columnNamesParam)
			}
			columnNames = append(columnNames, str)
		}
	}

	prefix := defaultPrefix
	if prefixRaw, ok := params[prefixParam]; ok {
		prefix, ok = prefixRaw.(string)
		if !ok {
			return CSVParams{}, fmt.Errorf("%v must be string", prefixParam)
		}
	}

	delimiter := defaultDelimiter
	if delimiterRaw, ok := params[delimiterParam]; ok {
		delimiterStr, ok := delimiterRaw.(string)
		if !ok {
			return CSVParams{}, fmt.Errorf("%v must be string", delimiterParam)
		}

		delimiter = byte(delimiterStr[0])
	}

	return CSVParams{
		columnNames: columnNames,
		prefix:      prefix,
		delimiter:   delimiter,
	}, nil
}

func validDelim(r byte) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n'
}

func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}
