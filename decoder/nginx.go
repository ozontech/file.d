package decoder

import (
	"errors"

	insaneJSON "github.com/vitkovskii/insane-json"
)

type NginxErrorRow struct {
	Time    []byte
	Level   []byte
	Message []byte
}

// DecodeNginxError decodes nginx error formatted log and merges result with event.
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
//		"message": "2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer"
//	}
func DecodeNginxError(event *insaneJSON.Root, data []byte) error {
	row, err := DecodeNginxErrorTo(data)
	if err != nil {
		return err
	}

	event.AddFieldNoAlloc(event, "time").MutateToBytesCopy(event, row.Time)
	event.AddFieldNoAlloc(event, "level").MutateToBytesCopy(event, row.Level)
	if len(row.Message) > 0 {
		event.AddFieldNoAlloc(event, "message").MutateToBytesCopy(event, row.Message)
	}

	return nil
}

// DecodeNginxErrorTo decodes nginx error formated log to [NginxErrorRow].
//
// Example of format:
//
//	"2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer"
func DecodeNginxErrorTo(data []byte) (NginxErrorRow, error) {
	row := NginxErrorRow{}

	split := spaceSplit(data, 3)
	if len(split) < 3 {
		return row, errors.New("incorrect format, missing required fields")
	}

	row.Time = data[:split[1]]
	if split[2]-split[1] < 4 {
		return row, errors.New("incorrect log level format, too short")
	}

	row.Level = data[split[1]+2 : split[2]-1]

	if len(data) > split[2] {
		row.Message = data[split[2]+1:]
	}

	return row, nil
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
