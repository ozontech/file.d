package decoder

import (
	"bytes"
	"fmt"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
)

type SyslogRFC3164Row struct {
	Priority  []byte
	Facility  string
	Severity  string
	Timestamp []byte
	Hostname  []byte
	AppName   []byte
	ProcID    []byte
	Message   []byte
}

type syslogRFC3164Decoder struct {
	params syslogParams
}

func NewSyslogRFC3164Decoder(params map[string]any) (Decoder, error) {
	p, err := extractSyslogParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &syslogRFC3164Decoder{
		params: p,
	}, nil
}

func (d *syslogRFC3164Decoder) Type() Type {
	return SYSLOG_RFC3164
}

// DecodeToJson decodes syslog-RFC3164 formatted log and merges result with root.
//
// From:
//
//	"<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"
//
// To:
//
//	{
//		"priority": "34",
//		"facility": "4",
//		"severity": "2",
//		"timestamp": "Oct  5 22:14:15",
//		"hostname": "mymachine.example.com",
//		"app_name": "myproc",
//		"process_id": "10",
//		"message": "'myproc' failed on /dev/pts/8"
//	}
func (d *syslogRFC3164Decoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	rowRaw, err := d.Decode(data)
	if err != nil {
		return err
	}
	row := rowRaw.(SyslogRFC3164Row)

	syslogDecodeToJson(root, SyslogRFC5424Row{SyslogRFC3164Row: row})
	return nil
}

// Decode decodes syslog-RFC3164 formatted log to [SyslogRFC3164Row].
//
// Example of format:
//
//	"<34>Oct 11 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"
func (d *syslogRFC3164Decoder) Decode(data []byte, _ ...any) (any, error) {
	var row SyslogRFC3164Row

	data = bytes.TrimSuffix(data, []byte("\n"))
	if len(data) == 0 {
		return row, errSyslogInvalidFormat
	}

	// priority
	pri, offset, err := syslogParsePriority(data)
	if err != nil {
		return row, fmt.Errorf("failed to parse priority: %w", err)
	}
	// offset points to '>'
	row.Priority = data[1:offset]
	data = data[offset+1:]

	// facility & severity from priority
	row.Facility = syslogFacilityFromPriority(pri, d.params.facilityFormat)
	row.Severity = syslogSeverityFromPriority(pri, d.params.severityFormat)

	// timestamp
	if !d.validateTimestamp(data) {
		return row, fmt.Errorf("failed to parse timestamp: %w", errSyslogInvalidTimestamp)
	}
	row.Timestamp = data[:len(time.Stamp)]
	data = data[len(time.Stamp)+1:]

	// hostname
	offset = bytes.IndexByte(data, ' ')
	if offset < 0 {
		return row, fmt.Errorf("failed to parse hostname: %w", errSyslogInvalidFormat)
	}
	row.Hostname = data[:offset]
	data = data[offset+1:]

	// appname
	offset = bytes.IndexAny(data, "[: ")
	if offset < 0 {
		return row, fmt.Errorf("failed to parse appname: %w", errSyslogInvalidFormat)
	}
	row.AppName = data[:offset]
	data = data[offset:]

	// optional procid
	if data[0] == '[' {
		offset = bytes.IndexByte(data, ']')
		if offset < 0 || data[offset+1] != ':' {
			return row, fmt.Errorf("failed to parse ProcID: %w", errSyslogInvalidFormat)
		}
		row.ProcID = data[1:offset]
		data = data[offset+2:]
	} else {
		data = data[1:]
	}

	// message
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	row.Message = data

	return row, nil
}

// validateTimestamp validates [time.Stamp] format and trailing space (Jan _2 15:04:05 )
func (d *syslogRFC3164Decoder) validateTimestamp(ts []byte) bool {
	if len(ts) < len(time.Stamp)+1 {
		return false
	}
	if !(ts[3] == ' ' && ts[6] == ' ' && ts[9] == ':' && ts[12] == ':' && ts[15] == ' ') {
		return false
	}
	// Mmm
	if ts[0] < 'A' || 'Z' < ts[0] ||
		ts[1] < 'a' || ts[1] > 'z' ||
		ts[2] < 'a' || ts[2] > 'z' {
		return false
	}
	// dd
	if !((ts[4] == ' ' || isDigit(ts[4])) && isDigit(ts[5])) {
		return false
	}
	// time
	if !(checkNumber(ts[7:9], 0, 23) && checkNumber(ts[10:12], 0, 59) && checkNumber(ts[13:15], 0, 59)) {
		return false
	}

	return true
}
