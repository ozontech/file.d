package decoder

import (
	"bytes"
	"errors"
	"fmt"
	"unicode"

	insaneJSON "github.com/ozontech/insane-json"
)

const (
	nginxWithCustomFieldsParam = "nginx_with_custom_fields"
)

type NginxErrorRow struct {
	Time         []byte
	Level        []byte
	PID          []byte
	TID          []byte
	CID          []byte
	Message      []byte
	CustomFields map[string][]byte
}

type nginxErrorParams struct {
	withCustomFields bool // optional
}

type nginxErrorDecoder struct {
	params nginxErrorParams
}

func NewNginxErrorDecoder(params map[string]any) (Decoder, error) {
	p, err := extractNginxErrorParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &nginxErrorDecoder{
		params: p,
	}, nil
}

func (d *nginxErrorDecoder) Type() Type {
	return NGINX_ERROR
}

// DecodeToJson decodes nginx error formatted log and merges result with root.
//
// From:
//
//	"2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer"
//
// To:
//
//	{
//		"time": "2022/08/17 10:49:27",
//		"level": "error",
//		"pid": "2725122",
//		"tid": "2725122",
//		"cid": "792412315",
//		"message": "lua udp socket read timed out, context: ngx.timer"
//	}
func (d *nginxErrorDecoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	rowRaw, err := d.Decode(data)
	if err != nil {
		return err
	}
	row := rowRaw.(NginxErrorRow)

	root.AddFieldNoAlloc(root, "time").MutateToBytesCopy(root, row.Time)
	root.AddFieldNoAlloc(root, "level").MutateToBytesCopy(root, row.Level)
	root.AddFieldNoAlloc(root, "pid").MutateToBytesCopy(root, row.PID)
	root.AddFieldNoAlloc(root, "tid").MutateToBytesCopy(root, row.TID)
	if len(row.CID) > 0 {
		root.AddFieldNoAlloc(root, "cid").MutateToBytesCopy(root, row.CID)
	}
	if len(row.Message) > 0 {
		root.AddFieldNoAlloc(root, "message").MutateToBytesCopy(root, row.Message)
	}
	for k, v := range row.CustomFields {
		root.AddFieldNoAlloc(root, k).MutateToBytesCopy(root, v)
	}

	return nil
}

// Decode decodes nginx error formated log to [NginxErrorRow].
//
// Example of format:
//
//	"2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer"
func (d *nginxErrorDecoder) Decode(data []byte, _ ...any) (any, error) {
	row := NginxErrorRow{}

	data = bytes.TrimSuffix(data, []byte("\n"))

	split := spaceSplit(data, 5)
	if len(split) < 4 {
		return row, errors.New("incorrect format, missing required fields")
	}

	row.Time = data[:split[1]]
	if split[2]-split[1] < 4 {
		return row, errors.New("incorrect log level format, too short")
	}

	row.Level = data[split[1]+2 : split[2]-1]

	pidComplete := false
	tidComplete := false
	for i := split[2] + 1; i < split[3]; i++ {
		if data[i] == '#' {
			pidComplete = true
			continue
		}
		if data[i] == ':' {
			tidComplete = true
			break
		}
		if pidComplete {
			row.TID = append(row.TID, data[i])
		} else {
			row.PID = append(row.PID, data[i])
		}
	}
	if !(pidComplete && tidComplete) {
		return row, errors.New("incorrect log pid#tid format")
	}

	if len(data) <= split[3]+1 {
		return row, nil
	}

	if len(split) > 4 && data[split[3]+1] == '*' {
		row.CID = data[split[3]+2 : split[4]]
		if len(data) > split[4]+1 {
			row.Message, row.CustomFields = d.extractCustomFields(data[split[4]+1:])
		}
	} else {
		row.Message, row.CustomFields = d.extractCustomFields(data[split[3]+1:])
	}

	return row, nil
}

// extractCustomFields extracts custom fields from nginx error log message.
//
// Example of input:
//
//	`upstream timed out (110: Operation timed out) while connecting to upstream, client: 10.125.172.251, server: , request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"`
//
// Example of output:
//
//	message: "upstream timed out (110: Operation timed out) while connecting to upstream"
//	fields:
//		"client": "10.125.172.251"
//		"server": ""
//		"request": "POST /download HTTP/1.1"
//		"upstream": "http://10.117.246.15:84/download"
//		"host": "mpm-youtube-downloader-38.name.tldn:84"
func (d *nginxErrorDecoder) extractCustomFields(data []byte) ([]byte, map[string][]byte) {
	if !d.params.withCustomFields {
		return data, nil
	}

	fields := make(map[string][]byte)
	for len(data) > 0 {
		sepIdx := bytes.LastIndex(data, []byte(", "))
		if sepIdx == -1 {
			break
		}
		field := data[sepIdx+2:] // `key: value` format

		idx := bytes.IndexByte(field, ':')
		if idx == -1 {
			break
		}
		key := field[:idx]

		// check key contains only letters
		if bytes.ContainsFunc(key, func(r rune) bool {
			return !unicode.IsLetter(r)
		}) {
			break
		}

		value := field[idx+2:]
		if len(value) > 0 {
			value = bytes.Trim(value, `"`)
		}
		fields[string(key)] = value
		data = data[:sepIdx]
	}

	return data, fields
}

func extractNginxErrorParams(params map[string]any) (nginxErrorParams, error) {
	withCustomFields := false
	if withCustomFieldsRaw, ok := params[nginxWithCustomFieldsParam]; ok {
		withCustomFields, ok = withCustomFieldsRaw.(bool)
		if !ok {
			return nginxErrorParams{}, fmt.Errorf("%q must be bool", nginxWithCustomFieldsParam)
		}
	}

	return nginxErrorParams{
		withCustomFields: withCustomFields,
	}, nil
}

func spaceSplit(b []byte, limit int) []int {
	var res []int
	for i := 0; i < len(b) && len(res) < limit; i++ {
		if b[i] == ' ' {
			res = append(res, i)
		}
	}
	return res
}
