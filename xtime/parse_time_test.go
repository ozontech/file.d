package xtime

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTime(t *testing.T) {
	testTime := time.Now()
	tests := []struct {
		name   string
		format string
		value  string
		want   int64
	}{
		{
			name:   "unixtime_ok",
			format: "unixtime",
			value:  strconv.FormatInt(testTime.Unix(), 10),
			want:   testTime.Unix() * 1e9,
		},
		{
			name:   "unixtimemilli_ok",
			format: "unixtimemilli",
			value:  strconv.FormatInt(testTime.UnixMilli(), 10),
			want:   testTime.UnixMilli() * 1e6,
		},
		{
			name:   "unixtimemicro_ok",
			format: "unixtimemicro",
			value:  strconv.FormatInt(testTime.UnixMicro(), 10),
			want:   testTime.UnixMicro() * 1e3,
		},
		{
			name:   "unixtimenano_ok",
			format: "unixtimenano",
			value:  strconv.FormatInt(testTime.UnixNano(), 10),
			want:   testTime.UnixNano(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTime(tt.format, tt.value)
			require.NoError(t, err, "must be no error")
			require.Equal(t, tt.want, got.UnixNano())
		})
	}
}
