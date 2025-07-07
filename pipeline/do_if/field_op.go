package do_if

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline/checker"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ do-if-field-op
`equal` checks whether the field value is equal to one of the elements in the values list.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: equal
          field: pod
          values: [test-pod-1, test-pod-2]
```

Result:
```
{"pod":"test-pod-1","service":"test-service"}   # discarded
{"pod":"test-pod-2","service":"test-service-2"} # discarded
{"pod":"test-pod","service":"test-service"}     # not discarded
{"pod":"test-pod","service":"test-service-1"}   # not discarded
```

`contains` checks whether the field value contains one of the elements the in values list.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: contains
          field: pod
          values: [my-pod, my-test]
```

Result:
```
{"pod":"test-my-pod-1","service":"test-service"}     # discarded
{"pod":"test-not-my-pod","service":"test-service-2"} # discarded
{"pod":"my-test-pod","service":"test-service"}       # discarded
{"pod":"test-pod","service":"test-service-1"}        # not discarded
```

`prefix` checks whether the field value has prefix equal to one of the elements in the values list.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: prefix
          field: pod
          values: [test-1, test-2]
```

Result:
```
{"pod":"test-1-pod-1","service":"test-service"}   # discarded
{"pod":"test-2-pod-2","service":"test-service-2"} # discarded
{"pod":"test-pod","service":"test-service"}       # not discarded
{"pod":"test-pod","service":"test-service-1"}     # not discarded
```

`suffix` checks whether the field value has suffix equal to one of the elements in the values list.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: suffix
          field: pod
          values: [pod-1, pod-2]
```

Result:
```
{"pod":"test-1-pod-1","service":"test-service"}   # discarded
{"pod":"test-2-pod-2","service":"test-service-2"} # discarded
{"pod":"test-pod","service":"test-service"}       # not discarded
{"pod":"test-pod","service":"test-service-1"}     # not discarded
```

`regex` checks whether the field matches any regex from the values list.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: regex
          field: pod
          values: [pod-\d, my-test.*]
```

Result:
```
{"pod":"test-1-pod-1","service":"test-service"}       # discarded
{"pod":"test-2-pod-2","service":"test-service-2"}     # discarded
{"pod":"test-pod","service":"test-service"}           # not discarded
{"pod":"my-test-pod","service":"test-service-1"}      # discarded
{"pod":"my-test-instance","service":"test-service-1"} # discarded
{"pod":"service123","service":"test-service-1"}       # not discarded
```

}*/

/*{ do-if-field-op-node
DoIf field op node is considered to always be a leaf in the DoIf tree. It checks byte representation of the value by the given field path.
Array and object values are considered as not matched since encoding them to bytes leads towards large CPU and memory consumption.

Params:
  - `op` - value from field operations list. Required.
  - `field` - path to field in JSON tree. If empty, root value is checked. Path to nested fields is delimited by dots `"."`, e.g. `"field.subfield"` for `{"field": {"subfield": "val"}}`.
  If the field name contains dots in it they should be shielded with `"\"`, e.g. `"exception\.type"` for `{"exception.type": "example"}`. Default empty.
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

type fieldOpNode struct {
	fieldPath    []string
	fieldPathStr string
	checker      *checker.Checker
}

func newFieldOpNode(op string, field string, caseSensitive bool, values [][]byte) (Node, error) {
	if len(values) == 0 {
		return nil, errors.New("values are not provided")
	}

	c, err := checker.New(op, caseSensitive, values)
	if err != nil {
		return nil, err
	}

	return &fieldOpNode{
		fieldPath:    cfg.ParseFieldSelector(field),
		fieldPathStr: field,
		checker:      c,
	}, nil
}

func (n *fieldOpNode) Type() nodeType {
	return NodeFieldOp
}

func (n *fieldOpNode) check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if node.IsArray() || node.IsObject() {
		return false
	}

	if node.IsNull() {
		return n.checker.Check(nil)
	}

	return n.checker.Check(node.AsBytes())
}

func (n *fieldOpNode) isEqualTo(n2 Node, _ int) error {
	n2f, ok := n2.(*fieldOpNode)
	if !ok {
		return errors.New("nodes have different types expected: fieldOpNode")
	}

	if n.fieldPathStr != n2f.fieldPathStr || slices.Compare[[]string](n.fieldPath, n2f.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr expected: fieldPathStr=%q fieldPath=%v",
			n.fieldPathStr, n.fieldPath,
		)
	}

	return checker.Equal(n.checker, n2f.checker)
}
