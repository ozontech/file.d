package matchrule

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type DoIfNodeType int

const (
	DoIfNodeEmpty DoIfNodeType = iota
	DoIfNodeFieldOp
	DoIfNodeLogicalOp
)

type DoIfNode interface {
	Type() DoIfNodeType
	Check(*insaneJSON.Root, map[string][]byte) bool

	getUniqueFields() map[string]struct{}
}

type logicalOpType int

const (
	logicalOpUnknown logicalOpType = iota
	logicalOr
	logicalAnd
	logicalNot
)

var (
	logicalOrBytes  = []byte(`or`)
	logicalAndBytes = []byte(`and`)
	logicalNotBytes = []byte(`not`)
)

type logicalNode struct {
	op       logicalOpType
	operands []DoIfNode
}

func NewLogicalNode(op []byte, operands []DoIfNode) (DoIfNode, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}
	var lop logicalOpType
	switch {
	case bytes.Equal(op, logicalOrBytes):
		lop = logicalOr
	case bytes.Equal(op, logicalAndBytes):
		lop = logicalAnd
	case bytes.Equal(op, logicalNotBytes):
		lop = logicalNot
		if len(operands) > 1 {
			return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
		}
	default:
		return nil, fmt.Errorf("unknown logical op %q", op)
	}
	return &logicalNode{
		op:       lop,
		operands: operands,
	}, nil
}

func (n *logicalNode) Type() DoIfNodeType {
	return DoIfNodeLogicalOp
}

func (n *logicalNode) Check(eventRoot *insaneJSON.Root, fieldsVals map[string][]byte) bool {
	switch n.op {
	case logicalOr:
		for _, op := range n.operands {
			if op.Check(eventRoot, fieldsVals) {
				return true
			}
		}
		return false
	case logicalAnd:
		for _, op := range n.operands {
			if !op.Check(eventRoot, fieldsVals) {
				return false
			}
		}
		return true
	case logicalNot:
		return !n.operands[0].Check(eventRoot, fieldsVals)
	}
	return false
}

func (n *logicalNode) getUniqueFields() map[string]struct{} {
	result := make(map[string]struct{})
	for _, op := range n.operands {
		for k := range op.getUniqueFields() {
			result[k] = struct{}{}
		}
	}
	return result
}

type fieldOpType int

const (
	unknownOp fieldOpType = iota
	equalOp
	containsOp
	prefixOp
	suffixOp
	regexOp
)

var (
	equalOpBytes    = []byte(`equal`)
	containsOpBytes = []byte(`contains`)
	prefixOpBytes   = []byte(`prefix`)
	suffixOpBytes   = []byte(`suffix`)
	regexOpBytes    = []byte(`regex`)
)

type fieldOpNode struct {
	op            fieldOpType
	fieldPath     []string
	fieldPathStr  string
	caseSensitive bool
	values        [][]byte
	valuesBySize  map[int][][]byte
	reValues      []*regexp.Regexp

	minValLen int
	maxValLen int
}

func NewFieldOpNode(op string, field string, caseSensitive bool, values [][]byte) (DoIfNode, error) {
	if field == "" {
		return nil, errors.New("field is not specified")
	}
	if len(values) == 0 {
		return nil, errors.New("values are not provided")
	}
	var vals [][]byte
	var valsBySize map[int][][]byte
	var reValues []*regexp.Regexp
	var minValLen, maxValLen int
	var fop fieldOpType

	fieldPath := cfg.ParseFieldSelector(field)

	opBytes := []byte(op)
	switch {
	case bytes.Equal(opBytes, equalOpBytes):
		fop = equalOp
	case bytes.Equal(opBytes, containsOpBytes):
		fop = containsOp
	case bytes.Equal(opBytes, prefixOpBytes):
		fop = prefixOp
	case bytes.Equal(opBytes, suffixOpBytes):
		fop = suffixOp
	case bytes.Equal(opBytes, regexOpBytes):
		fop = regexOp
		reValues = make([]*regexp.Regexp, 0, len(values))
		for _, v := range values {
			re, err := regexp.Compile(string(v))
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex %q: %w", v, err)
			}
			reValues = append(reValues, re)
		}
	default:
		return nil, fmt.Errorf("unknown field op %q", op)
	}

	if fop != regexOp {
		minValLen = len(values[0])
		maxValLen = len(values[0])
		if fop == equalOp {
			valsBySize = make(map[int][][]byte)
		} else {
			vals = make([][]byte, len(values))
		}
		for i := range values {
			var curVal []byte
			if values[i] != nil {
				curVal = make([]byte, len(values[i]))
				copy(curVal, values[i])
			}
			if !caseSensitive {
				curVal = bytes.ToLower(curVal)
			}
			if len(values[i]) < minValLen {
				minValLen = len(values[i])
			}
			if len(values[i]) > maxValLen {
				maxValLen = len(values[i])
			}
			if fop == equalOp {
				valsBySize[len(curVal)] = append(valsBySize[len(curVal)], curVal)
			} else {
				vals[i] = curVal
			}
		}
	}

	return &fieldOpNode{
		op:            fop,
		fieldPath:     fieldPath,
		fieldPathStr:  field,
		caseSensitive: caseSensitive,
		values:        vals,
		valuesBySize:  valsBySize,
		reValues:      reValues,
		minValLen:     minValLen,
		maxValLen:     maxValLen,
	}, nil
}

func (n *fieldOpNode) Type() DoIfNodeType {
	return DoIfNodeFieldOp
}

func (n *fieldOpNode) Check(eventRoot *insaneJSON.Root, fieldsVals map[string][]byte) bool {
	data, ok := fieldsVals[n.fieldPathStr]
	if !ok {
		node := eventRoot.Dig(n.fieldPath...)
		data = node.AsBytes()
		fieldsVals[n.fieldPathStr] = data
	}
	// fast check for data
	if n.op != regexOp && len(data) < n.minValLen {
		return false
	}
	switch n.op {
	case equalOp:
		vals, ok := n.valuesBySize[len(data)]
		if !ok {
			return false
		}
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range vals {
			if bytes.Equal(data, val) {
				return true
			}
		}
	case containsOp:
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.Contains(data, val) {
				return true
			}
		}
	case prefixOp:
		// check only necessary amount of bytes
		if len(data) > n.maxValLen {
			data = data[:n.maxValLen]
		}
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.HasPrefix(data, val) {
				return true
			}
		}
	case suffixOp:
		// check only necessary amount of bytes
		if len(data) > n.maxValLen {
			data = data[len(data)-n.maxValLen:]
		}
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.HasSuffix(data, val) {
				return true
			}
		}
	case regexOp:
		for _, re := range n.reValues {
			if re.Match(data) {
				return true
			}
		}
	}
	return false
}

func (n *fieldOpNode) getUniqueFields() map[string]struct{} {
	return map[string]struct{}{
		n.fieldPathStr: {},
	}
}

type DoIfChecker struct {
	root            DoIfNode
	uniqueFieldsLen int
	procsFieldsVals map[int]map[string][]byte
	mu              sync.RWMutex
}

func NewDoIfChecker(root DoIfNode) *DoIfChecker {
	uniqueFields := root.getUniqueFields()
	return &DoIfChecker{
		root:            root,
		uniqueFieldsLen: len(uniqueFields),
		procsFieldsVals: make(map[int]map[string][]byte),
	}
}

func (c *DoIfChecker) getProcFieldsVals(procID int) map[string][]byte {
	c.mu.RLock()
	data, ok := c.procsFieldsVals[procID]
	if ok {
		c.mu.RUnlock()
		return data
	}
	c.mu.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	data = make(map[string][]byte, c.uniqueFieldsLen)
	c.procsFieldsVals[procID] = data
	return data
}

func (c *DoIfChecker) Check(eventRoot *insaneJSON.Root, procID int) bool {
	if eventRoot == nil {
		return false
	}
	fieldsVals := c.getProcFieldsVals(procID)
	result := c.root.Check(eventRoot, fieldsVals)
	for k := range fieldsVals {
		delete(fieldsVals, k)
	}
	return result
}
