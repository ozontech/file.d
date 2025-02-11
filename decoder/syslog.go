package decoder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	insaneJSON "github.com/ozontech/insane-json"
)

var (
	errSyslogInvalidFormat    = errors.New("log doesn't conform the format")
	errSyslogInvalidPriority  = errors.New("PRI header not a valid priority")
	errSyslogInvalidTimestamp = errors.New("timestamp doesn't conform the format")
	errSyslogInvalidVersion   = errors.New("version doesn't conform the format")
	errSyslogInvalidSD        = errors.New("structured data doesn't conform the format")
)

const (
	syslogFacilityFormatParam = "syslog_facility_format"
	syslogSeverityFormatParam = "syslog_severity_format"

	// priority = facility * 8 + severity.
	// max facility = 23, max severity = 7.
	// 23 * 8 + 7 = 191.
	syslogMaxPriority = 191
)

var bom = []byte{0xEF, 0xBB, 0xBF}

type syslogParams struct {
	facilityFormat string // optional
	severityFormat string // optional
}

func extractSyslogParams(params map[string]any) (syslogParams, error) {
	facilityFormat := spfNumber
	if facilityFormatRaw, ok := params[syslogFacilityFormatParam]; ok {
		facilityFormat, ok = facilityFormatRaw.(string)
		if !ok {
			return syslogParams{}, fmt.Errorf("%q must be string", syslogFacilityFormatParam)
		}
		if err := syslogPriorityFormatValidate(syslogFacilityFormatParam, facilityFormat); err != nil {
			return syslogParams{}, err
		}
	}

	severityFormat := spfNumber
	if severityFormatRaw, ok := params[syslogSeverityFormatParam]; ok {
		severityFormat, ok = severityFormatRaw.(string)
		if !ok {
			return syslogParams{}, fmt.Errorf("%q must be string", syslogSeverityFormatParam)
		}
		if err := syslogPriorityFormatValidate(syslogSeverityFormatParam, severityFormat); err != nil {
			return syslogParams{}, err
		}
	}

	return syslogParams{
		facilityFormat: facilityFormat,
		severityFormat: severityFormat,
	}, nil
}

const (
	spfNumber = "number"
	spfString = "string"
)

func syslogPriorityFormatValidate(param, format string) error {
	switch format {
	case spfNumber, spfString:
		return nil
	default:
		return fmt.Errorf("invalid %q format, must be one of [number|string]", param)
	}
}

func syslogParsePriority(data []byte) (int, int, error) {
	if len(data) < 3 || data[0] != '<' {
		return 0, 0, errSyslogInvalidFormat
	}
	offset := bytes.IndexByte(data, '>')
	if offset < 2 || 4 < offset {
		return 0, 0, errSyslogInvalidFormat
	}
	p, ok := atoi(data[1:offset])
	if !ok || p > syslogMaxPriority {
		return 0, 0, errSyslogInvalidPriority
	}
	return p, offset, nil
}

func syslogFacilityFromPriority(p int, format string) string {
	f := p / 8
	if format == spfNumber {
		return strconv.Itoa(f)
	}
	return syslogFacilityString(f)
}

func syslogSeverityFromPriority(p int, format string) string {
	s := p % 8
	if format == spfNumber {
		return strconv.Itoa(s)
	}
	return syslogSeverityString(s)
}

func syslogFacilityString(f int) string {
	switch f {
	case 0:
		return "KERN"
	case 1:
		return "USER"
	case 2:
		return "MAIL"
	case 3:
		return "DAEMON"
	case 4:
		return "AUTH"
	case 5:
		return "SYSLOG"
	case 6:
		return "LPR"
	case 7:
		return "NEWS"
	case 8:
		return "UUCP"
	case 9:
		return "CRON"
	case 10:
		return "AUTHPRIV"
	case 11:
		return "FTP"
	case 12:
		return "NTP"
	case 13:
		return "SECURITY"
	case 14:
		return "CONSOLE"
	case 15:
		return "SOLARISCRON"
	case 16:
		return "LOCAL0"
	case 17:
		return "LOCAL1"
	case 18:
		return "LOCAL2"
	case 19:
		return "LOCAL3"
	case 20:
		return "LOCAL4"
	case 21:
		return "LOCAL5"
	case 22:
		return "LOCAL6"
	case 23:
		return "LOCAL7"
	default:
		return "UNKNOWN"
	}
}

func syslogSeverityString(s int) string {
	switch s {
	case 0:
		return "EMERG"
	case 1:
		return "ALERT"
	case 2:
		return "CRIT"
	case 3:
		return "ERROR"
	case 4:
		return "WARN"
	case 5:
		return "NOTICE"
	case 6:
		return "INFO"
	case 7:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

type SyslogSDParams map[string][]byte
type SyslogSD map[string]SyslogSDParams

func syslogDecodeToJson(root *insaneJSON.Root, row SyslogRFC5424Row) { // nolint: gocritic // hugeParam is ok
	root.AddFieldNoAlloc(root, "priority").MutateToBytesCopy(root, row.Priority)
	root.AddFieldNoAlloc(root, "facility").MutateToString(row.Facility)
	root.AddFieldNoAlloc(root, "severity").MutateToString(row.Severity)
	if len(row.ProtoVersion) > 0 {
		root.AddFieldNoAlloc(root, "proto_version").MutateToBytesCopy(root, row.ProtoVersion)
	}
	if len(row.Timestamp) > 0 {
		root.AddFieldNoAlloc(root, "timestamp").MutateToBytesCopy(root, row.Timestamp)
	}
	if len(row.Hostname) > 0 {
		root.AddFieldNoAlloc(root, "hostname").MutateToBytesCopy(root, row.Hostname)
	}
	if len(row.AppName) > 0 {
		root.AddFieldNoAlloc(root, "app_name").MutateToBytesCopy(root, row.AppName)
	}
	if len(row.ProcID) > 0 {
		root.AddFieldNoAlloc(root, "process_id").MutateToBytesCopy(root, row.ProcID)
	}
	if len(row.MsgID) > 0 {
		root.AddFieldNoAlloc(root, "message_id").MutateToBytesCopy(root, row.MsgID)
	}
	if len(row.Message) > 0 {
		root.AddFieldNoAlloc(root, "message").MutateToBytesCopy(root, row.Message)
	}

	for id, params := range row.StructuredData {
		if len(params) == 0 {
			continue
		}
		obj := root.AddFieldNoAlloc(root, id).MutateToObject()
		for k, v := range params {
			obj.AddFieldNoAlloc(root, k).MutateToBytesCopy(root, v)
		}
	}
}
