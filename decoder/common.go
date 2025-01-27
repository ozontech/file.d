package decoder

import (
	"encoding/json"
	"errors"
	"math"
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
		return 0, errors.New("not int")
	}
}

// atoi is allocation free ASCII number to integer conversion
func atoi(b []byte) (int, error) {
	res := 0
	pos := 0
	for i := len(b) - 1; i >= 0; i-- {
		c := int(b[i]) - '0'
		if c > 10 {
			return 0, errors.New("not a number")
		}
		res += c * int(math.Pow10(pos))
		pos++
	}
	return res, nil
}
