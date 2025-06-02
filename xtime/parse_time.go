package xtime

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type unixTimeFormat int

const (
	unixTimeSec unixTimeFormat = iota
	unixTimeMilli
	unixTimeMicro
	unixTimeNano
)

func ParseTime(format, value string) (time.Time, error) {
	switch format {
	case UnixTime:
		return parseUnixTime(value, unixTimeSec)
	case UnixTimeMilli:
		return parseUnixTime(value, unixTimeMilli)
	case UnixTimeMicro:
		return parseUnixTime(value, unixTimeMicro)
	case UnixTimeNano:
		return parseUnixTime(value, unixTimeNano)
	default:
		return time.Parse(format, value)
	}
}

func parseUnixTime(value string, format unixTimeFormat) (time.Time, error) {
	numbers := strings.Split(value, ".")
	var sec, nsec, val int64
	var err error
	switch len(numbers) {
	case 1:
		val, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		switch format {
		case unixTimeSec:
			sec = val
		case unixTimeMilli:
			sec = val / 1e3
			nsec = (val % 1e3) * 1e6
		case unixTimeMicro:
			sec = val / 1e6
			nsec = (val % 1e6) * 1e3
		case unixTimeNano:
			sec = val / 1e9
			nsec = val % 1e9
		}
	case 2:
		// when timestamp is presented as a float number its whole part is always considered as seconds
		// and the fractional part is fractions of a second
		sec, err = strconv.ParseInt(numbers[0], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		nsec, err = strconv.ParseInt(numbers[1], 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		// if there are less than 9 digits to the right of the decimal point
		// it must be multiplied by 10^(9 - digits) to get nsec value
		digits := len(numbers[1])
		if digits < 9 {
			nsec *= int64(math.Pow10(9 - digits))
		}
	default:
		return time.Time{}, fmt.Errorf("unexpected time format")
	}
	return time.Unix(sec, nsec), nil
}
