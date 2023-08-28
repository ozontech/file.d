package xtime

import (
	"sync/atomic"
	"time"
)

const updateTimeInterval = time.Second

var nowTime atomic.Int64

func GetInaccurateTimeUnix() int64 {
	return nowTime.Load()
}

func GetInaccurateTime() time.Time {
	return time.Unix(0, nowTime.Load())
}

func Start() {
	ticker := time.NewTicker(updateTimeInterval)
	for {
		select {
		case t := <-ticker.C:
			setNowTime(t)
		}
	}
}

func setNowTime(t time.Time) {
	nowTime.Store(t.UnixNano())
}
