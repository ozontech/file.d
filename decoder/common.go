package decoder

import (
	"encoding/json"
	"errors"
)

func anyToInt(v any) (int, error) {
	switch vNum := v.(type) {
	case int:
		return vNum, nil
	case float64:
		return int(vNum), nil
	case json.Number:
		vInt64, err := vNum.Int64()
		if err != nil {
			return 0, err
		}
		return int(vInt64), nil
	default:
		return 0, errors.New("value is not convertable to int")
	}
}

// atoi is allocation free ASCII number to integer conversion
func atoi(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	x := 0
	for _, c := range b {
		if c < '0' || '9' < c {
			return 0, false
		}
		x = x*10 + int(c) - '0'
	}
	return x, true
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func checkNumber(num []byte, minimum, maximum int) bool {
	x, ok := atoi(num)
	return ok && x >= minimum && x <= maximum
}
