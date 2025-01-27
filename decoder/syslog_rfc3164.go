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
	PID       []byte
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

// DecodeSyslogRFC3164ToJson decodes syslog-RFC3164 formatted log and merges result with root.
//
// From:
//
//	"<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"
//
// To:
//
//	{
//		"priority": "34",
//		"facility": 4,
//		"severity": 2,
//		"timestamp": "Oct  5 22:14:15",
//		"hostname": "mymachine.example.com",
//		"app_name": "myproc",
//		"pid": "10",
//		"message": "'myproc' failed on /dev/pts/8"
//	}
func (d *syslogRFC3164Decoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	rowRaw, err := d.Decode(data)
	if err != nil {
		return err
	}
	row := rowRaw.(SyslogRFC3164Row)

	root.AddFieldNoAlloc(root, "priority").MutateToBytesCopy(root, row.Priority)
	root.AddFieldNoAlloc(root, "facility").MutateToString(row.Facility)
	root.AddFieldNoAlloc(root, "severity").MutateToString(row.Severity)
	root.AddFieldNoAlloc(root, "timestamp").MutateToBytesCopy(root, row.Timestamp)
	root.AddFieldNoAlloc(root, "hostname").MutateToBytesCopy(root, row.Hostname)
	root.AddFieldNoAlloc(root, "app_name").MutateToBytesCopy(root, row.AppName)
	if len(row.PID) > 0 {
		root.AddFieldNoAlloc(root, "pid").MutateToBytesCopy(root, row.PID)
	}
	if len(row.Message) > 0 {
		root.AddFieldNoAlloc(root, "message").MutateToBytesCopy(root, row.Message)
	}

	return nil
}

// Decode decodes syslog-RFC3164 formatted log to [SyslogRFC3164Row].
//
// Example of format:
//
//	"<34>Oct 11 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"
func (d *syslogRFC3164Decoder) Decode(data []byte, _ ...any) (any, error) {
	var (
		row    SyslogRFC3164Row
		offset int
		err    error
	)

	data = bytes.TrimSuffix(data, []byte("\n"))
	if len(data) == 0 {
		return row, errSyslogInvalidFormat
	}

	// priority
	row.Priority, offset, err = syslogParsePriority(data)
	if err != nil {
		return row, err
	}
	pri, err := atoi(row.Priority)
	// max facility = 23, max severity = 7. 23 * 8 + 7 = 191
	if err != nil || pri > 191 {
		return row, errSyslogInvalidPriority
	}
	row.Facility = syslogFacilityFromPriority(pri, d.params.facilityFormat)
	row.Severity = syslogSeverityFromPriority(pri, d.params.severityFormat)
	data = data[offset:]

	// timestamp
	if !d.validateTimestamp(data) {
		return row, errSyslogInvalidTimestamp
	}
	row.Timestamp = data[:len(time.Stamp)]
	data = data[len(time.Stamp)+1:]

	// hostname
	offset = bytes.IndexByte(data, ' ')
	if offset < 0 {
		return row, errSyslogInvalidFormat
	}
	row.Hostname = data[:offset]
	data = data[offset+1:]

	// appname
	offset = bytes.IndexAny(data, "[: ")
	if offset < 0 {
		return row, errSyslogInvalidFormat
	}
	row.AppName = data[:offset]
	data = data[offset:]

	// optional pid
	if data[0] == '[' {
		offset = bytes.IndexByte(data, ']')
		if offset < 0 || data[offset+1] != ':' {
			return row, errSyslogInvalidFormat
		}
		row.PID = data[1:offset]
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

// validateTimestamp valiudates time.Stamp format and trailing space (Jan _2 15:04:05 )
func (d *syslogRFC3164Decoder) validateTimestamp(ts []byte) bool {
	isDigit := func(c byte) bool {
		return c >= '0' && c <= '9'
	}

	if len(ts) < len(time.Stamp)+1 {
		return false
	}
	if ts[3] != ' ' || ts[6] != ' ' || ts[9] != ':' || ts[12] != ':' || ts[15] != ' ' {
		return false
	}
	// Mmm
	if ts[0] < 'A' || 'Z' < ts[0] ||
		ts[1] < 'a' || ts[1] > 'z' ||
		ts[2] < 'a' || ts[2] > 'z' {
		return false
	}
	// dd
	if !(ts[4] == ' ' || isDigit(ts[4])) || !isDigit(ts[5]) {
		return false
	}
	// hh, mm, ss
	if !isDigit(ts[7]) || !isDigit(ts[8]) ||
		!isDigit(ts[10]) || !isDigit(ts[11]) ||
		!isDigit(ts[13]) || !isDigit(ts[14]) {
		return false
	}

	return true
}
