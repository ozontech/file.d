package antispam

type nodeType int

const (
	nodeTypeValue nodeType = iota
	nodeTypeLogical
)

type Node interface {
	Type() nodeType
	check(event []byte, sourceName []byte, metadata map[string]string) bool
}
