package clickhouse

import (
	insaneJSON "github.com/vitkovskii/insane-json"
)

type InsaneNode interface {
	AsInt() (int, error)
	AsString() (string, error)
	AsBool() (bool, error)
	AsInt64() (int64, error)
	AsStringArray() ([]string, error)
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

func nonStrictAsString(node *insaneJSON.Node) string {
	var val string
	if node.IsString() {
		val = node.AsString()
	} else {
		val = node.EncodeToString()
	}
	return val
}
