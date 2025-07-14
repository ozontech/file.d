package antispam

import (
	"fmt"
	"strings"

	"github.com/ozontech/file.d/pipeline/checker"
)

type dataType int

const (
	dataTypeEvent dataType = iota
	dataTypeSourceName
	dataTypeMeta
)

func (c dataType) String() string {
	switch c {
	case dataTypeEvent:
		return dataTypeEventTag
	case dataTypeSourceName:
		return dataTypeSourceNameTag
	case dataTypeMeta:
		return dataTypeMetaTag
	default:
		panic(fmt.Sprintf("unknown checked data type: %d", c))
	}
}

const (
	dataTypeEventTag      = "event"
	dataTypeSourceNameTag = "source_name"
	dataTypeMetaTag       = "meta"

	metaTagPrefix = "meta:"
)

func stringToDataType(s string) (dataType, string) {
	switch {
	case s == dataTypeEventTag:
		return dataTypeEvent, ""
	case s == dataTypeSourceNameTag:
		return dataTypeSourceName, ""
	case strings.HasPrefix(s, metaTagPrefix):
		return dataTypeMeta, strings.TrimPrefix(s, metaTagPrefix)
	default:
		panic(fmt.Sprintf("unparsable check data tag: %s", s))
	}
}

type valueNode struct {
	dataType dataType
	metaKey  string
	checker  *checker.Checker
}

func newValueNode(
	opTag string,
	caseSensitive bool,
	values [][]byte,
	checkDataTag string,
	metaKey string,
) *valueNode {
	c, err := checker.New(opTag, caseSensitive, values)
	if err != nil {
		panic(fmt.Sprintf("init checker: %s", err.Error()))
	}

	dType, metaKey := stringToDataType(checkDataTag)

	return &valueNode{
		dataType: dType,
		metaKey:  metaKey,
		checker:  c,
	}
}

func (n *valueNode) getType() nodeType {
	return nodeTypeValue
}

func (n *valueNode) check(event []byte, sourceName []byte, metadata map[string]string) bool {
	switch n.dataType {
	case dataTypeEvent:
		return n.checker.Check(event)
	case dataTypeSourceName:
		return n.checker.Check(sourceName)
	case dataTypeMeta:
		data, ok := metadata[n.metaKey]
		return ok && n.checker.Check([]byte(data))
	default:
		panic(fmt.Sprintf("inknown type of checked data: %d", n.dataType))
	}
}
