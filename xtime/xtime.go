package xtime

import (
	"sync"
	"sync/atomic"
	"time"
)

const updateTimeInterval = time.Second

var nowTime atomic.Int64

func GetInaccurateUnixNano() int64 {
	startOnce()
	return nowTime.Load()
}

func GetInaccurateTime() time.Time {
	return time.Unix(0, GetInaccurateUnixNano())
}

var startOnce = sync.OnceFunc(func() {
	setNowTime(time.Now())
	ticker := time.NewTicker(updateTimeInterval)
	go func() {
		for t := range ticker.C {
			setNowTime(t)
		}
	}()
})

func setNowTime(t time.Time) {
	nowTime.Store(t.UnixNano())
}
