package decoder

import (
	"errors"

	insaneJSON "github.com/vitkovskii/insane-json"
)

type NginxErrorRow struct {
	Time    []byte
	Level   []byte
	PID     []byte
	TID     []byte
	CID     []byte
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
	event.AddFieldNoAlloc(event, "pid").MutateToBytesCopy(event, row.PID)
	event.AddFieldNoAlloc(event, "tid").MutateToBytesCopy(event, row.TID)
	if len(row.CID) > 0 {
		event.AddFieldNoAlloc(event, "cid").MutateToBytesCopy(event, row.CID)
	}
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

	if len(data) > split[3]+1 {
		if len(split) > 4 && data[split[3]+1] == '*' {
			row.CID = data[split[3]+2 : split[4]]
			if len(data) > split[4]+1 {
				row.Message = data[split[4]+1:]
			}
		} else {
			row.Message = data[split[3]+1:]
		}
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
