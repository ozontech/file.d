package antispam

import (
	"errors"
	"fmt"

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
)

func stringToDataType(s string) (dataType, error) {
	switch s {
	case dataTypeEventTag:
		return dataTypeEvent, nil
	case dataTypeSourceNameTag:
		return dataTypeSourceName, nil
	case dataTypeMetaTag:
		return dataTypeMeta, nil
	default:
		return -1, fmt.Errorf("unknown checked type data: %s", s)
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
) (*valueNode, error) {
	c, err := checker.New(opTag, caseSensitive, values)
	if err != nil {
		return nil, fmt.Errorf("init checker: %w", err)
	}

	var dType dataType
	dType, err = stringToDataType(checkDataTag)
	if err != nil {
		return nil, err
	}

	if dType == dataTypeMeta {
		if metaKey == "" {
			return nil, errors.New("empty meta key")
		}
	}

	return &valueNode{
		dataType: dType,
		metaKey:  metaKey,
		checker:  c,
	}, nil
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
