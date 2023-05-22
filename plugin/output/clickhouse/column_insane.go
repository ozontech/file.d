package clickhouse

import insaneJSON "github.com/vitkovskii/insane-json"

type InsaneNode interface {
	AsInt() (int, error)
	AsString() (string, error)
	AsBool() (bool, error)
	AsInt64() (int64, error)
	EncodeToString() string

	IsNumber() bool
	IsString() bool
	IsNull() bool
}

var (
	_ InsaneNode = (*NonStrictNode)(nil)
	_ InsaneNode = (*insaneJSON.StrictNode)(nil)
)

type NonStrictNode struct {
	node *insaneJSON.Node
}

func (n NonStrictNode) AsInt() (int, error) {
	return n.node.AsInt(), nil
}

func (n NonStrictNode) AsString() (string, error) {
	return n.node.AsString(), nil
}

func (n NonStrictNode) AsBool() (bool, error) {
	return n.node.AsBool(), nil
}

func (n NonStrictNode) EncodeToString() string {
	return n.node.EncodeToString()
}

func (n NonStrictNode) AsInt64() (int64, error) {
	return n.node.AsInt64(), nil
}

func (n NonStrictNode) IsNumber() bool {
	return n.node.IsNumber()
}

func (n NonStrictNode) IsString() bool {
	return n.node.IsString()
}

func (n NonStrictNode) IsNull() bool {
	return n.node.IsNull()
}
