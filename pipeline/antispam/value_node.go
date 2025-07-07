package antispam

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline/checker"
)

type checkData int

const (
	checkDataEvent checkData = iota
	checkDataSourceName
	checkDataMeta
)

func (c checkData) String() string {
	switch c {
	case checkDataEvent:
		return checkDataEventTag
	case checkDataSourceName:
		return checkDataSourceNameTag
	case checkDataMeta:
		return checkDataMetaTag
	default:
		panic(fmt.Sprintf("unknown checked data type: %d", c))
	}
}

const (
	checkDataEventTag      = "event"
	checkDataSourceNameTag = "source_name"
	checkDataMetaTag       = "meta"
)

func stringToCheckData(s string) (checkData, error) {
	switch s {
	case checkDataEventTag:
		return checkDataEvent, nil
	case checkDataSourceNameTag:
		return checkDataSourceName, nil
	case checkDataMetaTag:
		return checkDataMeta, nil
	default:
		return -1, fmt.Errorf("unknown checked type data: %s", s)
	}
}

type valueNode struct {
	checkData checkData
	metaKey   string
	checker   *checker.Checker
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

	var checkDataType checkData
	checkDataType, err = stringToCheckData(checkDataTag)
	if err != nil {
		return nil, err
	}

	if checkDataType == checkDataMeta {
		if metaKey == "" {
			return nil, errors.New("empty meta key")
		}
	}

	return &valueNode{
		checkData: checkDataType,
		metaKey:   metaKey,
		checker:   c,
	}, nil
}

func (n *valueNode) Type() nodeType {
	return nodeTypeUsual
}

func (n *valueNode) check(event []byte, sourceName []byte, metadata map[string]string) bool {
	switch n.checkData {
	case checkDataEvent:
		return n.checker.Check(event)
	case checkDataSourceName:
		return n.checker.Check(sourceName)
	case checkDataMeta:
		data, ok := metadata[n.metaKey]
		return ok && n.checker.Check([]byte(data))
	default:
		panic(fmt.Sprintf("inknown type of checked data: %d", n.checkData))
	}
}
