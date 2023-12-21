package cfg

import (
	"math"
	"time"
)

func GetExponentDuration(minRetention time.Duration, attemptNum int) time.Duration {
	sleep := time.Duration(math.Pow(2, float64(attemptNum))) * minRetention
	return sleep
}
