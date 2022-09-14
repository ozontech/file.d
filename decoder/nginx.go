package decoder

import (
	"errors"
	"fmt"
	"time"

	insaneJSON "github.com/vitkovskii/insane-json"
)

const (
	nginxDateFmt = "2006/01/02 15:04:05"
	parseDate    = false
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
	if parseDate {
		d, err := time.Parse(nginxDateFmt, string(tBuf))
		if err != nil {
			return fmt.Errorf("date in wrong format=%s", string(tBuf))
		}
		tBuf = []byte(d.Format(nginxDateFmt))
	}

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
