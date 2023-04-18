package file_clickhouse

import (
	"encoding/json"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type Sample struct {
	C1       json.RawMessage            `json:"c1"`
	C2       int8                       `json:"c2"`
	C3       int16                      `json:"c3"`
	C4       proto.Nullable[int16]      `json:"c4"`
	C5       proto.Nullable[string]     `json:"c5"`
	Level    proto.Enum8                `json:"level"`
	IPv4     proto.Nullable[proto.IPv4] `json:"ipv4"`
	IPv6     proto.Nullable[proto.IPv6] `json:"ipv6"`
	TS       time.Time                  `json:"ts"`
	TSWithTZ time.Time                  `json:"ts_with_tz"`
	TS64     time.Time                  `json:"ts_64"`
	TS64Auto time.Time                  `json:"ts_64_auto"`
}

var _ json.Marshaler = (*Sample)(nil)

// Enum8('error'=1, 'warn'=2, 'info'=3, 'debug'=4),
var levelToString = map[proto.Enum8]string{
	1: "error",
	2: "warn",
	3: "info",
	4: "debug",
}

func (s *Sample) MarshalJSON() ([]byte, error) {
	ipv4 := ""
	if s.IPv4.Set {
		ipv4 = s.IPv4.Value.String()
	}

	ipv6 := ""
	if s.IPv6.Set {
		ipv6 = s.IPv6.Value.String()
	}

	return json.Marshal(struct {
		C1       json.RawMessage `json:"c1,omitempty"`
		C2       int8            `json:"c2,omitempty"`
		C3       int16           `json:"c3,omitempty"`
		C4       int16           `json:"c4,omitempty"`
		C5       string          `json:"c5,omitempty"`
		Level    string          `json:"level,omitempty"`
		Ipv4     string          `json:"ipv4,omitempty"`
		Ipv6     string          `json:"ipv6,omitempty"`
		TS       int64           `json:"ts"`
		TSWithTZ int64           `json:"ts_with_tz"`
		TS64     int64           `json:"ts64"`
	}{
		C1:       s.C1,
		C2:       s.C2,
		C3:       s.C3,
		C4:       s.C4.Value,
		C5:       s.C5.Value,
		Level:    levelToString[s.Level],
		Ipv4:     ipv4,
		Ipv6:     ipv6,
		TS:       s.TS.Unix(),
		TSWithTZ: s.TS.Unix(),
		TS64:     s.TS64.UnixMilli(),
	})
}
