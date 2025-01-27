package decoder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

var (
	errSyslogInvalidFormat    = errors.New("log doesn't conform the format")
	errSyslogInvalidPriority  = errors.New("PRI header not a valid priority")
	errSyslogInvalidTimestamp = errors.New("timestamp doesn't conform the format")
)

const (
	syslogFacilityFormatParam = "syslog_facility_format"
	syslogSeverityFormatParam = "syslog_severity_format"
)

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

func syslogParsePriority(data []byte) ([]byte, int, error) {
	offset := 0
	if data[offset] != '<' {
		return nil, offset, errSyslogInvalidFormat
	}
	offset = bytes.IndexByte(data, '>')
	if offset < 2 || 4 < offset {
		return nil, offset, errSyslogInvalidFormat
	}

	return data[1:offset], offset + 1, nil
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
