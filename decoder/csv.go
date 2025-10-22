package decoder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/ozontech/file.d/logger"
	insaneJSON "github.com/ozontech/insane-json"
)

type InvalidLineMode string

const (
	columnNamesParam     = "columns"
	prefixParam          = "prefix"
	delimiterParam       = "delimiter"
	invalidLineModeParam = "invalid_line_mode"

	InvalidLineModeFatal    InvalidLineMode = "fatal"
	InvalidLineModeContinue InvalidLineMode = "continue"
	InvalidLineModeDefault  InvalidLineMode = "default"

	defaultPrefix    = ""
	defaultDelimiter = byte(',')
)

type CSVRow []string

type CSVParams struct {
	columnNames     []string
	prefix          string
	invalidLineMode InvalidLineMode
	delimiter       byte
}

type CSVBuffers struct {
	recordBuffer []byte
	fieldIndexes []int
	resultBuffer CSVRow
}

func NewCSVBuffers() *CSVBuffers {
	return &CSVBuffers{
		recordBuffer: make([]byte, 0),
		fieldIndexes: make([]int, 0),
		resultBuffer: make(CSVRow, 0),
	}
}

type CSVDecoder struct {
	params      CSVParams
	buffersPool sync.Pool
}

func NewCSVDecoder(params map[string]any) (Decoder, error) {
	p, err := extractCSVParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &CSVDecoder{
		params: p,
		buffersPool: sync.Pool{
			New: func() any {
				return NewCSVBuffers()
			},
		},
	}, nil
}

func (d *CSVDecoder) Type() Type {
	return CSV
}

// DecodeToJson decodes csv formatted log and merges result with root.
//
// From (columns: ['timestamp', 'ip', 'service', 'info'], delimiter: ";"):
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
// From (columns: [], prefix: 'csv_', delimiter: ","):
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

	if len(d.params.columnNames) != 0 {
		if len(row) != len(d.params.columnNames) {
			switch d.params.invalidLineMode {
			case InvalidLineModeFatal:
				logger.Fatalf("got invalid line with setting InvalidLineMode=%s", InvalidLineModeFatal)
			case InvalidLineModeContinue:
			default:
				return errors.New("wrong number of fields")
			}
		}
	}

	var columnName string
	for i := range row {
		if i < len(d.params.columnNames) {
			columnName = d.params.columnNames[i]
		} else {
			columnName = d.params.prefix + strconv.Itoa(i)
		}
		root.AddFieldNoAlloc(root, columnName).MutateToString(row[i])
	}

	return nil
}

// Decode decodes CSV formatted log to []string.
//
// Example of format:
//
// "1760551019001	127.0.0.1	example-service	some-additional-info"
func (d *CSVDecoder) Decode(data []byte, _ ...any) (any, error) {
	if n := len(data); n >= 2 && data[n-2] == '\r' && data[n-1] == '\n' {
		data[n-2] = '\n'
		data = data[:n-1]
	}

	buffers := d.buffersPool.Get().(*CSVBuffers)
	defer d.buffersPool.Put(buffers)

	const quoteLen = 1
	const delimiterLen = 1

	buffers.recordBuffer = buffers.recordBuffer[:0]
	buffers.fieldIndexes = buffers.fieldIndexes[:0]
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

			buffers.recordBuffer = append(buffers.recordBuffer, field...)
			buffers.fieldIndexes = append(buffers.fieldIndexes, len(buffers.recordBuffer))
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
					buffers.recordBuffer = append(buffers.recordBuffer, data[:i]...)
					data = data[i+quoteLen:]
					switch rn := data[0]; {
					case rn == '"':
						// `""` sequence (append quote).
						buffers.recordBuffer = append(buffers.recordBuffer, '"')
						data = data[quoteLen:]
					case rn == d.params.delimiter:
						// `",` sequence (end of field).
						data = data[delimiterLen:]
						buffers.fieldIndexes = append(buffers.fieldIndexes, len(buffers.recordBuffer))
						continue parseField
					case lengthNL(data) == len(data):
						// `"\n` sequence (end of data).
						buffers.fieldIndexes = append(buffers.fieldIndexes, len(buffers.recordBuffer))
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

	str := string(buffers.recordBuffer)
	buffers.resultBuffer = buffers.resultBuffer[:0]
	if cap(buffers.resultBuffer) < len(buffers.fieldIndexes) {
		buffers.resultBuffer = make([]string, len(buffers.fieldIndexes))
	}
	buffers.resultBuffer = buffers.resultBuffer[:len(buffers.fieldIndexes)]
	var preIdx int
	for i, idx := range buffers.fieldIndexes {
		buffers.resultBuffer[i] = str[preIdx:idx]
		preIdx = idx
	}

	return buffers.resultBuffer, nil
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

	invalidLineMode := InvalidLineModeDefault
	if invalidLineModeRaw, ok := params[invalidLineModeParam]; ok {
		invalidLineModeStr, ok := invalidLineModeRaw.(string)
		if !ok {
			return CSVParams{}, fmt.Errorf("%v must be string", invalidLineModeParam)
		}

		invalidLineMode = InvalidLineMode(invalidLineModeStr)
	}

	delimiter := defaultDelimiter
	if delimiterRaw, ok := params[delimiterParam]; ok {
		delimiterStr, ok := delimiterRaw.(string)
		if !ok || len(delimiterStr) != 1 {
			return CSVParams{}, fmt.Errorf("%v must be string with length=1", delimiterParam)
		}

		delimiter = delimiterStr[0]
		if !validDelim(delimiter) {
			return CSVParams{}, fmt.Errorf("invalid delimiter")
		}
	}

	return CSVParams{
		columnNames:     columnNames,
		prefix:          prefix,
		invalidLineMode: invalidLineMode,
		delimiter:       delimiter,
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
