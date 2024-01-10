package xtime

import (
	"sync/atomic"
	"time"
)

func init() {
	SetNowTime(time.Now().UnixNano())
	ticker := time.NewTicker(updateTimeInterval)
	go func() {
		for t := range ticker.C {
			SetNowTime(t.UnixNano())
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

// SetNowTime sets the current time.
// Function should be used only in tests.
//
// An alternative to this approach is to store and redefine
// a function through the fields of the tested struct.
// But in this case, the inlining function GetInaccurateUnixNano is lost.
func SetNowTime(unixNano int64) {
	nowTime.Store(unixNano)
}
