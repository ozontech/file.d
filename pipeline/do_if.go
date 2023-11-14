package pipeline

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

// ! do-if-node
// ^ do-if-node

type DoIfNodeType int

const (
	DoIfNodeEmpty DoIfNodeType = iota

	// > Type of node where matching rules for fields are stored.
	DoIfNodeFieldOp // *

	// > Type of node where logical rules for applying other rules are stored.
	DoIfNodeLogicalOp // *
)

type DoIfNode interface {
	Type() DoIfNodeType
	Check(*insaneJSON.Root, map[string][]byte) bool

	getUniqueFields() map[string]struct{}
}

// ! do-if-field-op
// ^ do-if-field-op

type doIfFieldOpType int

const (
	doIfFieldUnknownOp doIfFieldOpType = iota
	doIfFieldEqualOp
	doIfFieldContainsOp
	doIfFieldPrefixOp
	doIfFieldSuffixOp
	doIfFieldRegexOp
)

var (
	// > checks whether the field value is equal to one of the elements in the values list.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - field_op: equal
	// >             field: pod
	// >             values: [test-pod-1, test-pod-2]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}     # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # not discarded
	// > ```
	doIfFieldEqualOpBytes = []byte(`equal`) // *

	// > checks whether the field value contains one of the elements the in values list.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - field_op: contains
	// >             field: pod
	// >             values: [my-pod, my-test]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-my-pod-1","service":"test-service"}     # discarded
	// > {"pod":"test-not-my-pod","service":"test-service-2"} # discarded
	// > {"pod":"my-test-pod","service":"test-service"}       # discarded
	// > {"pod":"test-pod","service":"test-service-1"}        # not discarded
	// > ```
	doIfFieldContainsOpBytes = []byte(`contains`) // *

	// > checks whether the field value has prefix equal to one of the elements in the values list.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - field_op: prefix
	// >             field: pod
	// >             values: [test-1, test-2]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-1-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-2-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}       # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}     # not discarded
	// > ```
	doIfFieldPrefixOpBytes = []byte(`prefix`) // *

	// > checks whether the field value has suffix equal to one of the elements in the values list.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - field_op: suffix
	// >             field: pod
	// >             values: [pod-1, pod-2]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-1-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-2-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}       # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}     # not discarded
	// > ```
	doIfFieldSuffixOpBytes = []byte(`suffix`) // *

	// > checks whether the field matches any regex from the values list.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - field_op: regex
	// >             field: pod
	// >             values: [pod-\d, my-test.*]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-1-pod-1","service":"test-service"}       # discarded
	// > {"pod":"test-2-pod-2","service":"test-service-2"}     # discarded
	// > {"pod":"test-pod","service":"test-service"}           # not discarded
	// > {"pod":"my-test-pod","service":"test-service-1"}      # discarded
	// > {"pod":"my-test-instance","service":"test-service-1"} # discarded
	// > {"pod":"service123","service":"test-service-1"}       # not discarded
	// > ```
	doIfFieldRegexOpBytes = []byte(`regex`) // *
)

/*{ do-if-field-op-node
DoIf field op node is considered to always be a leaf in the DoIf tree.
It contains operation to be checked on the field value, the field name to extract data and
the values to check against.

Params:
  - `field_op` - value from field operations list. Required.
  - `field` - name of the field to apply operation. Required.
  - `values` - list of values to check field. Required non-empty.
  - `case_sensitive` - flag indicating whether checks are performed in case sensitive way. Default `true`.
    Note: case insensitive checks can cause CPU and memory overhead since every field value will be converted to lower letters.

Example:
```yaml
pipelines:
  tests:
    actions:
      - type: discard
        do_if:
          - field_op: suffix
            field: pod
            values: [pod-1, pod-2]
            case_sensitive: true
```

}*/

type doIfFieldOpNode struct {
	op            doIfFieldOpType
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
	var fop doIfFieldOpType

	fieldPath := cfg.ParseFieldSelector(field)

	opBytes := []byte(op)
	switch {
	case bytes.Equal(opBytes, doIfFieldEqualOpBytes):
		fop = doIfFieldEqualOp
	case bytes.Equal(opBytes, doIfFieldContainsOpBytes):
		fop = doIfFieldContainsOp
	case bytes.Equal(opBytes, doIfFieldPrefixOpBytes):
		fop = doIfFieldPrefixOp
	case bytes.Equal(opBytes, doIfFieldSuffixOpBytes):
		fop = doIfFieldSuffixOp
	case bytes.Equal(opBytes, doIfFieldRegexOpBytes):
		fop = doIfFieldRegexOp
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

	if fop != doIfFieldRegexOp {
		minValLen = len(values[0])
		maxValLen = len(values[0])
		if fop == doIfFieldEqualOp {
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
			if fop == doIfFieldEqualOp {
				valsBySize[len(curVal)] = append(valsBySize[len(curVal)], curVal)
			} else {
				vals[i] = curVal
			}
		}
	}

	return &doIfFieldOpNode{
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

func (n *doIfFieldOpNode) Type() DoIfNodeType {
	return DoIfNodeFieldOp
}

func (n *doIfFieldOpNode) Check(eventRoot *insaneJSON.Root, fieldsVals map[string][]byte) bool {
	data, ok := fieldsVals[n.fieldPathStr]
	if !ok {
		node := eventRoot.Dig(n.fieldPath...)
		data = node.AsBytes()
		fieldsVals[n.fieldPathStr] = data
	}
	// fast check for data
	if n.op != doIfFieldRegexOp && len(data) < n.minValLen {
		return false
	}
	switch n.op {
	case doIfFieldEqualOp:
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
	case doIfFieldContainsOp:
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.Contains(data, val) {
				return true
			}
		}
	case doIfFieldPrefixOp:
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
	case doIfFieldSuffixOp:
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
	case doIfFieldRegexOp:
		for _, re := range n.reValues {
			if re.Match(data) {
				return true
			}
		}
	}
	return false
}

func (n *doIfFieldOpNode) getUniqueFields() map[string]struct{} {
	return map[string]struct{}{
		n.fieldPathStr: {},
	}
}

// ! do-if-logical-op
// ^ do-if-logical-op

type doIfLogicalOpType int

const (
	doIfLogicalOpUnknown doIfLogicalOpType = iota
	doIfLogicalOr
	doIfLogicalAnd
	doIfLogicalNot
)

var (
	// > accepts at least one operand and returns true on the first returned true from its operands.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - logical_op: or
	// >             operands:
	// >               - field_op: equal
	// >                 field: pod
	// >                 values: [test-pod-1, test-pod-2]
	// >               - field_op: equal
	// >                 field: service
	// >                 values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}     # discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # not discarded
	// > ```
	doIfLogicalOrBytes = []byte(`or`) // *

	// > accepts at least one operand and returns true if all operands return true
	// > (in other words returns false on the first returned false from its operands).
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - logical_op: and
	// >             operands:
	// >               - field_op: equal
	// >                 field: pod
	// >                 values: [test-pod-1, test-pod-2]
	// >               - field_op: equal
	// >                 field: service
	// >                 values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # not discarded
	// > {"pod":"test-pod","service":"test-service"}     # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # not discarded
	// > ```
	doIfLogicalAndBytes = []byte(`and`) // *

	// > accepts exactly one operand and returns inverted result of its operand.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           - logical_op: not
	// >             operands:
	// >               - field_op: equal
	// >                 field: service
	// >                 values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # not discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}     # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # discarded
	// > ```
	doIfLogicalNotBytes = []byte(`not`) // *
)

/*{ do-if-logical-op-node
DoIf logical op node is a node considered to be the root or an edge between nodes.
It always has at least one operand which are other nodes and calls their checks
to apply logical operation on their results.

Params:
  - `logical_op` - value from logical operations list. Required.
  - `operands` - list of another do-if nodes. Required non-empty.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          - logical_op: and
            operands:
              - field_op: equal
                field: pod
                values: [test-pod-1, test-pod-2]
                case_sensitive: true
              - field_op: equal
                field: service
                values: [test-service]
                case_sensitive: true
```

}*/

type doIfLogicalNode struct {
	op       doIfLogicalOpType
	operands []DoIfNode
}

func NewLogicalNode(op []byte, operands []DoIfNode) (DoIfNode, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}
	var lop doIfLogicalOpType
	switch {
	case bytes.Equal(op, doIfLogicalOrBytes):
		lop = doIfLogicalOr
	case bytes.Equal(op, doIfLogicalAndBytes):
		lop = doIfLogicalAnd
	case bytes.Equal(op, doIfLogicalNotBytes):
		lop = doIfLogicalNot
		if len(operands) > 1 {
			return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
		}
	default:
		return nil, fmt.Errorf("unknown logical op %q", op)
	}
	return &doIfLogicalNode{
		op:       lop,
		operands: operands,
	}, nil
}

func (n *doIfLogicalNode) Type() DoIfNodeType {
	return DoIfNodeLogicalOp
}

func (n *doIfLogicalNode) Check(eventRoot *insaneJSON.Root, fieldsVals map[string][]byte) bool {
	switch n.op {
	case doIfLogicalOr:
		for _, op := range n.operands {
			if op.Check(eventRoot, fieldsVals) {
				return true
			}
		}
		return false
	case doIfLogicalAnd:
		for _, op := range n.operands {
			if !op.Check(eventRoot, fieldsVals) {
				return false
			}
		}
		return true
	case doIfLogicalNot:
		return !n.operands[0].Check(eventRoot, fieldsVals)
	}
	return false
}

func (n *doIfLogicalNode) getUniqueFields() map[string]struct{} {
	result := make(map[string]struct{})
	for _, op := range n.operands {
		for k := range op.getUniqueFields() {
			result[k] = struct{}{}
		}
	}
	return result
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
