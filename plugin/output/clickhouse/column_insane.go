package clickhouse

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type InsaneNode interface {
	AsInt() (int, error)
	AsFloat32() (float32, error)
	AsFloat64() (float64, error)
	AsString() (string, error)
	AsBool() (bool, error)
	AsInt64() (int64, error)
	AsStringArray() ([]string, error)
	AsUUID() (uuid.UUID, error)
	AsIPv4() (proto.IPv4, error)
	AsIPv6() (proto.IPv6, error)
	AsTime() (time.Time, error)

	EncodeToString() string

	IsNumber() bool
	IsString() bool
	IsNull() bool
	IsArray() bool
}

var (
	_ InsaneNode = (*NonStrictNode)(nil)
	_ InsaneNode = (*StrictNode)(nil)
)

type StrictNode struct {
	*insaneJSON.StrictNode
}

func (s StrictNode) AsFloat32() (float32, error) {
	v, err := s.AsFloat()
	return float32(v), err
}

func (s StrictNode) AsFloat64() (float64, error) {
	return s.AsFloat()
}

func (s StrictNode) AsUUID() (uuid.UUID, error) {
	uuidRaw, err := s.AsString()
	if err != nil {
		return uuid.Nil, err
	}
	val, err := uuid.Parse(uuidRaw)
	if err != nil {
		return uuid.Nil, err
	}
	return val, nil
}

func (s StrictNode) AsIPv4() (proto.IPv4, error) {
	v, err := s.AsString()
	if err != nil {
		return 0, fmt.Errorf("node isn't string")
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return 0, fmt.Errorf("extract ip form json node val=%q: %w", v, err)
	}

	return proto.ToIPv4(addr), nil
}

func (s StrictNode) AsIPv6() (proto.IPv6, error) {
	v, err := s.AsString()
	if err != nil {
		return proto.IPv6{}, fmt.Errorf("node isn't string")
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return proto.IPv6{}, fmt.Errorf("extract ip form json node val=%q: %w", v, err)
	}

	return proto.ToIPv6(addr), nil
}

func (s StrictNode) AsTime() (time.Time, error) {
	switch {
	case s.IsNumber():
		nodeVal, err := s.AsInt64()
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(nodeVal, 0), nil
	case s.IsString():
		return parseRFC3339Nano(s)
	default:
		return time.Time{}, fmt.Errorf("value=%q is not a string or number", s.EncodeToString())
	}
}

func (s StrictNode) AsStringArray() ([]string, error) {
	arr, err := s.AsArray()
	if err != nil {
		return nil, err
	}
	vals := make([]string, len(arr))
	for i, n := range arr {
		vals[i], err = n.MutateToStrict().AsString()
		if err != nil {
			return nil, err
		}
	}
	return vals, nil
}

type NonStrictNode struct {
	*insaneJSON.Node
}

func (n NonStrictNode) AsStringArray() ([]string, error) {
	if n.Node == nil || n.Node.IsNull() {
		return []string{}, nil
	}

	var vals []string
	if n.IsArray() {
		arr := n.AsArray()
		vals = make([]string, len(arr))
		for i, n := range arr {
			vals[i] = nonStrictAsString(n)
		}
	} else {
		vals = []string{n.EncodeToString()}
	}
	return vals, nil
}

func (n NonStrictNode) AsInt() (int, error) {
	return n.Node.AsInt(), nil
}

func (n NonStrictNode) AsFloat32() (float32, error) {
	return float32(n.AsFloat()), nil
}

func (n NonStrictNode) AsFloat64() (float64, error) {
	return n.AsFloat(), nil
}

func (n NonStrictNode) AsString() (string, error) {
	if n.IsNil() || n.IsNull() {
		return "", nil
	}
	return nonStrictAsString(n.Node), nil
}

func (n NonStrictNode) AsBool() (bool, error) {
	return n.Node.AsBool(), nil
}

func (n NonStrictNode) EncodeToString() string {
	return n.Node.EncodeToString()
}

func (n NonStrictNode) AsInt64() (int64, error) {
	return n.Node.AsInt64(), nil
}

func (n NonStrictNode) AsUUID() (uuid.UUID, error) {
	uuidRaw, err := n.AsString()
	if err != nil {
		return uuid.Nil, nil
	}
	val, err := uuid.Parse(uuidRaw)
	if err != nil {
		return uuid.Nil, nil
	}
	return val, nil
}

func (n NonStrictNode) AsIPv4() (proto.IPv4, error) {
	v, err := n.AsString()
	if err != nil {
		return 0, nil
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return 0, nil
	}

	return proto.ToIPv4(addr), nil
}

func (n NonStrictNode) AsIPv6() (proto.IPv6, error) {
	v, err := n.AsString()
	if err != nil {
		return proto.IPv6{}, nil
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return proto.IPv6{}, nil
	}

	return proto.ToIPv6(addr), nil
}

func (n NonStrictNode) AsTime() (time.Time, error) {
	switch {
	case n.IsNumber():
		nodeVal, err := n.AsInt64()
		if err != nil {
			return time.Time{}, nil
		}
		return time.Unix(nodeVal, 0), nil
	case n.IsString():
		t, err := parseRFC3339Nano(n)
		if err != nil {
			return time.Time{}, nil
		}
		return t, nil
	default:
		return time.Time{}, nil
	}
}

func nonStrictAsString(node *insaneJSON.Node) string {
	var val string
	if node.IsString() {
		val = node.AsString()
	} else {
		val = node.EncodeToString()
	}
	return val
}

func parseRFC3339Nano(node InsaneNode) (time.Time, error) {
	nodeVal, err := node.AsString()
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse(time.RFC3339Nano, nodeVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time as RFC3339Nano: %w", err)
	}
	return t, nil
}
