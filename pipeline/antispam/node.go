package antispam

type nodeType int

const (
	nodeTypeUsual nodeType = iota
	nodeTypeLogical
)

type Node interface {
	Type() nodeType
	check(event []byte, sourceName []byte, metadata map[string]string) bool
}
