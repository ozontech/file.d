package doif

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

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

func (t doIfFieldOpType) String() string {
	switch t {
	case doIfFieldEqualOp:
		return "equal"
	case doIfFieldContainsOp:
		return "contains"
	case doIfFieldPrefixOp:
		return "prefix"
	case doIfFieldSuffixOp:
		return "suffix"
	case doIfFieldRegexOp:
		return "regex"
	}
	return "unknown"
}

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
	// >           op: equal
	// >           field: pod
	// >           values: [test-pod-1, test-pod-2]
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
	// >           op: contains
	// >           field: pod
	// >           values: [my-pod, my-test]
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
	// >           op: prefix
	// >           field: pod
	// >           values: [test-1, test-2]
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
	// >           op: suffix
	// >           field: pod
	// >           values: [pod-1, pod-2]
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
	// >           op: regex
	// >           field: pod
	// >           values: [pod-\d, my-test.*]
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
  - `op` - value from field operations list. Required.
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
          op: suffix
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

func NewFieldOpNode(op string, field string, caseSensitive bool, values [][]byte) (Node, error) {
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
			if !caseSensitive && curVal != nil {
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

func (n *doIfFieldOpNode) Type() NodeType {
	return NodeFieldOp
}

func (n *doIfFieldOpNode) Check(eventRoot *insaneJSON.Root) bool {
	var data []byte
	node := eventRoot.Dig(n.fieldPath...)
	if !node.IsNull() {
		data = node.AsBytes()
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
		if !n.caseSensitive && data != nil {
			data = bytes.ToLower(data)
		}
		for _, val := range vals {
			// null and empty strings are considered as different values
			// null can also come if field value is absent
			if (data == nil && val != nil) || (data != nil && val == nil) {
				continue
			}
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

func (n *doIfFieldOpNode) isEqualTo(n2 Node, _ int) error {
	n2f, ok := n2.(*doIfFieldOpNode)
	if !ok {
		return errors.New("nodes have different types expected: fieldOpNode")
	}
	if n.op != n2f.op {
		return fmt.Errorf("nodes have different op expected: %q", n.op)
	}
	if n.caseSensitive != n2f.caseSensitive {
		return fmt.Errorf("nodes have different caseSensitive expected: %v", n.caseSensitive)
	}
	if n.fieldPathStr != n2f.fieldPathStr || slices.Compare[[]string](n.fieldPath, n2f.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr expected: fieldPathStr=%q fieldPath=%v",
			n.fieldPathStr, n.fieldPath,
		)
	}
	if len(n.values) != len(n2f.values) {
		return fmt.Errorf("nodes have different values slices len expected: %d", len(n.values))
	}
	for i := 0; i < len(n.values); i++ {
		if !bytes.Equal(n.values[i], n2f.values[i]) {
			return fmt.Errorf("nodes have different data in values expected: %v on position", n.values)
		}
	}
	if len(n.valuesBySize) != len(n2f.valuesBySize) {
		return fmt.Errorf("nodes have different valuesBySize len expected: %d", len(n.valuesBySize))
	}
	for k, v := range n.valuesBySize {
		if v2, has := n2f.valuesBySize[k]; !has {
			return fmt.Errorf("nodes have different valuesBySize keys expected key: %d", k)
		} else if len(v) != len(v2) {
			return fmt.Errorf("nodes have different valuesBySize values len under key %d expected: %d", k, len(v))
		} else {
			for i := 0; i < len(v); i++ {
				if !bytes.Equal(v[i], v2[i]) {
					return fmt.Errorf("nodes have different valuesBySize data under key %d: %v", k, v)
				}
			}
		}
	}
	if len(n.reValues) != len(n2f.reValues) {
		return fmt.Errorf("nodes have different reValues len expected: %d", len(n.reValues))
	}
	for i := 0; i < len(n.reValues); i++ {
		if n.reValues[i].String() != n2f.reValues[i].String() {
			return fmt.Errorf("nodes have different reValues data expected: %v", n.reValues)
		}
	}
	if n.minValLen != n2f.minValLen {
		return fmt.Errorf("nodes have different minValLem expected: %d", n.minValLen)
	}
	if n.maxValLen != n2f.maxValLen {
		return fmt.Errorf("nodes have different maxValLem expected: %d", n.maxValLen)
	}
	return nil
}
