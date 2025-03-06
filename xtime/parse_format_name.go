package xtime

import (
	"fmt"
	"strings"
	"time"
)

const (
	formats       = "ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|unixtime|unixtimemilli|unixtimemicro|unixtimenano|nginx_errorlog"
	UnixTime      = "unixtime"
	UnixTimeMilli = "unixtimemilli"
	UnixTimeMicro = "unixtimemicro"
	UnixTimeNano  = "unixtimenano"
	nginxDateFmt  = "2006/01/02 15:04:05"
)

func ParseFormatName(formatName string) (string, error) {
	formatNameProcessed := strings.ToLower(strings.TrimSpace(formatName))
	switch formatNameProcessed {
	case "ansic":
		return time.ANSIC, nil
	case "unixdate":
		return time.UnixDate, nil
	case "rubydate":
		return time.RubyDate, nil
	case "rfc822":
		return time.RFC822, nil
	case "rfc822z":
		return time.RFC822Z, nil
	case "rfc850":
		return time.RFC850, nil
	case "rfc1123":
		return time.RFC1123, nil
	case "rfc1123z":
		return time.RFC1123Z, nil
	case "rfc3339":
		return time.RFC3339, nil
	case "rfc3339nano":
		return time.RFC3339Nano, nil
	case "kitchen":
		return time.Kitchen, nil
	case "stamp":
		return time.Stamp, nil
	case "stampmilli":
		return time.StampMilli, nil
	case "stampmicro":
		return time.StampMicro, nil
	case "stampnano":
		return time.StampNano, nil
	case "nginx_errorlog":
		return nginxDateFmt, nil
	case UnixTime, UnixTimeMilli, UnixTimeMicro, UnixTimeNano:
		return formatNameProcessed, nil
	default:
		return "", fmt.Errorf("unknown format name %q, should be one of %s", formatName, formats)
	}
}
