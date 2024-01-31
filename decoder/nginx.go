package decoder

import (
	"errors"

	insaneJSON "github.com/ozontech/insane-json"
)

func spaceSplit(b []byte, limit int) []int {
	var res []int
	for i := 0; i < len(b) && len(res) < limit; i++ {
		if b[i] == ' ' {
			res = append(res, i)
		}
	}
	return res
}

// DecodeNginxError decodes nginx error log format. Like:
// 2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer
func DecodeNginxError(event *insaneJSON.Root, data []byte) error {
	split := spaceSplit(data, 3)
	if len(split) < 3 {
		return errors.New("incorrect format, missing required fields")
	}

	tBuf := data[:split[1]]
	if split[2]-split[1] < 4 {
		return errors.New("incorrect log level format, too short")
	}

	event.AddFieldNoAlloc(event, "time").MutateToBytesCopy(event, tBuf)
	event.AddFieldNoAlloc(event, "level").MutateToBytesCopy(event, data[split[1]+2:split[2]-1]) // from nginx error level
	if len(data) > split[2] {
		event.AddFieldNoAlloc(event, "message").MutateToBytesCopy(event, data[split[2]+1:])
	}

	return nil
}
