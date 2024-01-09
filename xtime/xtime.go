package xtime

import (
	"sync/atomic"
	"time"
)

func init() {
	setNowTime(time.Now())
	ticker := time.NewTicker(updateTimeInterval)
	go func() {
		for t := range ticker.C {
			setNowTime(t)
		}
	}()
}

const updateTimeInterval = time.Second

var nowTime atomic.Int64

func GetInaccurateUnixNano() int64 {
	return nowTime.Load()
}

func GetInaccurateTime() time.Time {
	return time.Unix(0, GetInaccurateUnixNano())
}

func setNowTime(t time.Time) {
	nowTime.Store(t.UnixNano())
}
