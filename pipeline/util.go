package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func ByteToStringUnsafe(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StringToByteUnsafe(s string) []byte {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	var sh reflect.SliceHeader
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return *(*[]byte)(unsafe.Pointer(&sh))
}

type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

const formats = "ANSIC|UnixDate|RubyDate|RFC822|RFC822Z|RFC850|RFC1123|RFC1123Z|RFC3339|RFC3339Nano|Kitchen|Stamp|StampMilli|StampMicro|StampNano"

func ParseFormatName(formatName string) (string, error) {
	switch formatName {
	case "ANSIC":
		return time.ANSIC, nil
	case "UnixDate":
		return time.UnixDate, nil
	case "RubyDate":
		return time.RubyDate, nil
	case "RFC822":
		return time.RFC822, nil
	case "RFC822Z":
		return time.RFC822Z, nil
	case "RFC850":
		return time.RFC850, nil
	case "RFC1123":
		return time.RFC1123, nil
	case "RFC1123Z":
		return time.RFC1123Z, nil
	case "RFC3339":
		return time.RFC3339, nil
	case "RFC3339Nano":
		return time.RFC3339Nano, nil
	case "Kitchen":
		return time.Kitchen, nil
	case "Stamp":
		return time.Stamp, nil
	case "StampMilli":
		return time.StampMilli, nil
	case "StampMicro":
		return time.StampMicro, nil
	case "StampNano":
		return time.StampNano, nil
	default:
		return "", fmt.Errorf("unknown format name %q, should be one of %s", formatName, formats)
	}
}

func ParseLevel(level string) int {
	switch strings.ToUpper(level) {
	case "EMERGENCY":
		return 0
	case "ALERT":
		return 1
	case "CRITICAL", "CRIT":
		return 2
	case "ERROR", "ERR":
		return 3
	case "WARNING", "WARN":
		return 4
	case "NOTICE":
		return 5
	case "INFORMATIONAL", "INFO":
		return 6
	case "DEBUG":
		return 7
	default:
		return 6
	}
}

func TrimSpaceFunc(r rune) bool {
	return byte(r) == ' '
}
