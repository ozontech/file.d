package decoder

import (
	"bytes"
	"fmt"

	insaneJSON "github.com/ozontech/insane-json"
)

type SyslogRFC5424Row struct {
	SyslogRFC3164Row

	ProtoVersion   []byte
	MsgID          []byte
	StructuredData SyslogSD
}

type syslogRFC5424Decoder struct {
	params syslogParams
}

func NewSyslogRFC5424Decoder(params map[string]any) (Decoder, error) {
	p, err := extractSyslogParams(params)
	if err != nil {
		return nil, fmt.Errorf("can't extract params: %w", err)
	}

	return &syslogRFC5424Decoder{
		params: p,
	}, nil
}

func (d *syslogRFC5424Decoder) Type() Type {
	return SYSLOG_RFC5424
}

// DecodeToJson decodes syslog-RFC5424 formatted log and merges result with root.
//
// From:
//
//	`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log`
//
// To:
//
//	{
//		"priority": "165",
//		"facility": "20",
//		"severity": "5",
//		"proto_version": "1",
//		"timestamp": "2003-10-11T22:14:15.003Z",
//		"hostname": "mymachine.example.com",
//		"app_name": "myproc",
//		"process_id": "10",
//		"message_id": "ID47",
//		"message": "An application event log",
//		"exampleSDID@32473": {
//			"iut": "3",
//			"eventSource": "Application",
//			"eventID": "1011"
//		}
//	}
func (d *syslogRFC5424Decoder) DecodeToJson(root *insaneJSON.Root, data []byte) error {
	rowRaw, err := d.Decode(data)
	if err != nil {
		return err
	}
	row := rowRaw.(SyslogRFC5424Row)

	syslogDecodeToJson(root, row)
	return nil
}

// Decode decodes syslog-RFC5424 formatted log to [SyslogRFC5424Row].
//
// Example of format:
//
//	"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log"
func (d *syslogRFC5424Decoder) Decode(data []byte, _ ...any) (any, error) {
	var (
		row    SyslogRFC5424Row
		offset int
		ok     bool
		err    error
	)

	data = bytes.TrimSuffix(data, []byte("\n"))
	if len(data) == 0 {
		return row, errSyslogInvalidFormat
	}

	// priority
	pri, offset, err := syslogParsePriority(data)
	if err != nil {
		return row, err
	}
	row.Priority = data[1:offset]
	data = data[offset+1:]

	// facility & severity from priority
	row.Facility = syslogFacilityFromPriority(pri, d.params.facilityFormat)
	row.Severity = syslogSeverityFromPriority(pri, d.params.severityFormat)

	// proto version
	offset = bytes.IndexByte(data, ' ')
	if offset <= 0 {
		return row, errSyslogInvalidFormat
	}
	row.ProtoVersion = data[:offset]
	if _, ok = atoi(row.ProtoVersion); !ok {
		return row, errSyslogInvalidVersion
	}
	data = data[offset+1:]

	// timestamp
	offset, ok = d.readUntilSpaceOrNilValue(data)
	if !ok {
		return row, errSyslogInvalidFormat
	}
	if offset == 0 {
		data = data[2:]
	} else {
		row.Timestamp = data[:offset]
		if !d.validateTimestamp(row.Timestamp) {
			return row, errSyslogInvalidTimestamp
		}
		data = data[offset+1:]
	}

	// hostname
	offset, ok = d.readUntilSpaceOrNilValue(data)
	if !ok {
		return row, errSyslogInvalidFormat
	}
	if offset == 0 {
		data = data[2:]
	} else {
		row.Hostname = data[:offset]
		data = data[offset+1:]
	}

	// appname
	offset, ok = d.readUntilSpaceOrNilValue(data)
	if !ok {
		return row, errSyslogInvalidFormat
	}
	if offset == 0 {
		data = data[2:]
	} else {
		row.AppName = data[:offset]
		data = data[offset+1:]
	}

	// procid
	offset, ok = d.readUntilSpaceOrNilValue(data)
	if !ok {
		return row, errSyslogInvalidFormat
	}
	if offset == 0 {
		data = data[2:]
	} else {
		row.ProcID = data[:offset]
		data = data[offset+1:]
	}

	// msgid
	offset, ok = d.readUntilSpaceOrNilValue(data)
	if !ok {
		return row, errSyslogInvalidFormat
	}
	if offset == 0 {
		data = data[2:]
	} else {
		row.MsgID = data[:offset]
		data = data[offset+1:]
	}

	// structured data
	row.StructuredData, offset, ok = d.parseStructuredData(data)
	if !ok {
		return row, errSyslogInvalidSD
	}

	// no message
	if offset >= len(data) {
		return row, nil
	}
	data = data[offset+1:]

	// message
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	// BOM
	if len(data) > 2 && bytes.Equal(data[:3], bom) {
		data = data[3:]
	}
	row.Message = data

	return row, nil
}

// validateTimestamp validates [time.RFC3339] / [time.RFC3339Nano] formats
func (d *syslogRFC5424Decoder) validateTimestamp(ts []byte) bool {
	if len(ts) < len("2006-01-02T15:04:05Z") {
		return false
	}
	// format
	if !(ts[4] == '-' && ts[7] == '-' && ts[10] == 'T' && ts[13] == ':' && ts[16] == ':') {
		return false
	}
	// date
	if !(checkNumber(ts[:4], 0, 9999) && checkNumber(ts[5:7], 1, 12) && checkNumber(ts[8:10], 1, 31)) {
		return false
	}
	// time
	if !(checkNumber(ts[11:13], 0, 23) && checkNumber(ts[14:16], 0, 59) && checkNumber(ts[17:19], 0, 59)) {
		return false
	}
	// length of processed: "2006-01-02T15:04:05"
	ts = ts[19:]

	// nanoseconds
	if len(ts) >= 2 && ts[0] == '.' && isDigit(ts[1]) {
		i := 2
		for ; i < len(ts) && isDigit(ts[i]); i++ {
		}
		if i > 7 {
			return false
		}
		ts = ts[i:]
	}

	// timezone
	if len(ts) > 0 && ts[0] == 'Z' {
		return true
	}
	if len(ts) < len("-07:00") {
		return false
	}
	if !((ts[0] == '+' || ts[0] == '-') && ts[3] == ':') {
		return false
	}
	if !(checkNumber(ts[1:3], 0, 23) && checkNumber(ts[4:6], 0, 59)) {
		return false
	}

	return true
}

func (d *syslogRFC5424Decoder) parseStructuredData(data []byte) (SyslogSD, int, bool) {
	if len(data) > 0 && data[0] == '-' {
		return nil, 0, len(data) == 1 || data[1] == ' '
	}

	sd := SyslogSD{}
	offset := 0
	r := bytes.NewReader(nil)
	var (
		sdID, paramID                       string
		idx, startParamID, startParamValue  int
		wasOpen, wasClose, insideParamValue bool
	)

	shiftData := func(count int) {
		data = data[count:]
		offset += count
	}

	resetState := func() {
		r.Reset(data)
		idx = 0
		startParamID = 0
		startParamValue = 0
		insideParamValue = false
		paramID = ""
	}

	for len(data) > 0 {
		if data[0] != '[' {
			break
		}
		wasOpen = true
		shiftData(1)

		idx = bytes.IndexByte(data, ' ')
		if idx < 2 {
			return nil, 0, false
		}
		sdID = string(data[:idx])
		sd[sdID] = SyslogSDParams{}
		shiftData(idx + 1)

		resetState()
		wasClose = false
	paramsLoop:
		for {
			b, err := r.ReadByte()
			if err != nil {
				break
			}

			switch {
			case b == ']':
				if data[idx-1] != '"' {
					return nil, 0, false
				}
				wasClose = true
				break paramsLoop
			case b == ' ' && !insideParamValue:
				startParamID = idx + 1
			case b == '=' && !insideParamValue:
				if idx+1 < len(data) && data[idx+1] != '"' {
					return nil, 0, false
				}
				paramID = string(data[startParamID:idx])
			case b == '"':
				if data[idx-1] == '\\' {
					break
				}
				if insideParamValue {
					sd[sdID][paramID] = data[startParamValue:idx]
				} else {
					startParamValue = idx + 1
				}
				insideParamValue = !insideParamValue
			}
			idx++
		}
		if !wasClose {
			return nil, 0, false
		}
		shiftData(idx + 1)
	}

	if !wasOpen {
		return nil, 0, false
	}
	return sd, offset, true
}

// readUntilSpaceOrNilValue reads bytes until SP (' ') or NILVALUE ('-')
func (d *syslogRFC5424Decoder) readUntilSpaceOrNilValue(data []byte) (int, bool) {
	if len(data) < 2 {
		return -1, false
	}
	if data[0] == '-' && data[1] == ' ' {
		return 0, true
	}
	offset := bytes.IndexByte(data, ' ')
	return offset, offset > 0
}
