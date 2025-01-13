package clickhouse

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	insaneJSON "github.com/ozontech/insane-json"
)

var (
	ErrInvalidTimeType = errors.New("invalid node type for the time")
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
	AsTime(scale int64) (time.Time, error)

	IsNull() bool
}

var (
	_ InsaneNode = NonStrictNode{}
	_ InsaneNode = StrictNode{}
	_ InsaneNode = ZeroValueNode{}
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

func (s StrictNode) AsTime(scale int64) (time.Time, error) {
	return nodeAsTime(s.StrictNode, scale)
}

func (s StrictNode) AsStringArray() ([]string, error) {
	if s.StrictNode == nil || s.IsNull() {
		return nil, nil
	}

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
		return nil, nil
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

func (n NonStrictNode) AsTime(scale int64) (time.Time, error) {
	t, err := nodeAsTime(n.Node.MutateToStrict(), scale)
	if err != nil {
		return time.Time{}, nil
	}
	return t, nil
}

// ZeroValueNode returns a null-value for all called methods.
// It is usually used to insert a zero-value into a column
// if the field type of the event does not match the column type.
type ZeroValueNode struct{}

func (z ZeroValueNode) AsInt() (int, error) {
	return 0, nil
}

func (z ZeroValueNode) AsFloat32() (float32, error) {
	return 0, nil
}

func (z ZeroValueNode) AsFloat64() (float64, error) {
	return 0, nil
}

func (z ZeroValueNode) AsString() (string, error) {
	return "", nil
}

func (z ZeroValueNode) AsBool() (bool, error) {
	return false, nil
}

func (z ZeroValueNode) AsInt64() (int64, error) {
	return 0, nil
}

func (z ZeroValueNode) AsStringArray() ([]string, error) {
	return nil, nil
}

func (z ZeroValueNode) AsUUID() (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (z ZeroValueNode) AsIPv4() (proto.IPv4, error) {
	return proto.IPv4(0), nil
}

func (z ZeroValueNode) AsIPv6() (proto.IPv6, error) {
	return proto.IPv6{}, nil
}

func (z ZeroValueNode) AsTime(_ int64) (time.Time, error) {
	return time.Time{}, nil
}

func (z ZeroValueNode) IsNull() bool {
	return false
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

func nodeAsTime(n *insaneJSON.StrictNode, scale int64) (time.Time, error) {
	switch {
	case n.IsNumber():
		nodeVal, err := n.AsInt64()
		if err != nil {
			return time.Time{}, err
		}
		// convert to nanoseconds
		nsec := nodeVal * scale
		return time.Unix(nsec/1e9, nsec%1e9), nil
	case n.IsString():
		t, err := parseRFC3339Nano(n)
		if err != nil {
			return time.Time{}, err
		}
		return t, nil
	default:
		return time.Time{}, ErrInvalidTimeType
	}
}

func parseRFC3339Nano(node *insaneJSON.StrictNode) (time.Time, error) {
	nodeVal, err := node.AsString()
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse(time.RFC3339Nano, nodeVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing RFC3339Nano: %w", err)
	}
	return t, nil
}
