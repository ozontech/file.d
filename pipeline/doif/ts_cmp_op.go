package doif

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ do-if-ts-cmp-op-node
DoIf timestamp comparison op node is considered to always be a leaf in the DoIf tree like DoIf field op node.
It contains operation that compares timestamps with certain value.

Params:
  - `op` - must be `ts_cmp`. Required.
  - `field` - name of the field to apply operation. Required. Field will be parsed with `time.Parse` function.
  - `format` - format for timestamps representation. Required.
  - `cmp_op` - comparison operation name (same as for length comparison operations). Required.
  - `value` - timestamp value to compare field timestamps with. It must have `RFC3339Nano` format. Required.
Also, it may be `now` or `file_d_start`. If it is `now` then value to compare timestamps with is periodically updated current time.
If it is `file_d_start` then value to compare timestamps with will be program start moment.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: ts_cmp
          field: timestamp
          cmp_op: lt
          value: 2010-01-01T00:00:00Z
          format: 2006-01-02T15:04:05.999999999Z07:00
```

Result:
```
{"timestamp":"2000-01-01T00:00:00Z"}         # discarded
{"timestamp":"2008-01-01T00:00:00Z","id":1}  # discarded

{"pod_id":"some"}    # not discarded (no field `timestamp`)
{"timestamp":123}    # not discarded (field `timestamp` is not string)
{"timestamp":"qwe"}  # not discarded (field `timestamp` is not parsable)

{"timestamp":"2011-01-01T00:00:00Z"}  # not discarded (condition is not met)
```
}*/

type cmpValueChangingMode int

const (
	cmpValChModeConst cmpValueChangingMode = iota
	cmpValChModeNow
)

const (
	cmpValChModeConstTag = "const"
	cmpValChModeNowTag   = "now"
)

type tsCmpOpNode struct {
	fieldPath []string
	format    string

	cmpOp cmpOperation

	cmpValChMode   cmpValueChangingMode
	constCmpValue  int64
	varCmpValue    atomic.Int64
	updateInterval time.Duration
}

func NewTsCmpOpNode(field string, format string, cmpOp string, cmpValChMode string, cmpValue time.Time, updateInterval time.Duration) (Node, error) {
	typedCmpOp, err := newCmpOp(cmpOp)
	if err != nil {
		return nil, err
	}

	fieldPath := cfg.ParseFieldSelector(field)

	var resCmpValChMode cmpValueChangingMode
	switch cmpValChMode {
	case cmpValChModeNowTag:
		resCmpValChMode = cmpValChModeNow
	case cmpValChModeConstTag:
		resCmpValChMode = cmpValChModeConst
	default:
		return nil, fmt.Errorf("unknown ts cmp mode: %s", cmpValChMode)
	}

	result := &tsCmpOpNode{
		fieldPath:      fieldPath,
		format:         format,
		cmpOp:          typedCmpOp,
		cmpValChMode:   resCmpValChMode,
		constCmpValue:  cmpValue.UnixNano(),
		updateInterval: updateInterval,
	}
	result.startUpdater()

	return result, nil
}

func (n *tsCmpOpNode) startUpdater() {
	if n.cmpValChMode == cmpValChModeNow {
		n.varCmpValue.Store(time.Now().UnixNano())
		go func() {
			for {
				n.varCmpValue.Store(time.Now().UnixNano())
				time.Sleep(n.updateInterval)
			}
		}()
	}
}

func (n *tsCmpOpNode) Type() NodeType {
	return NodeTimestampCmpOp
}

func (n *tsCmpOpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if node == nil {
		return false
	}

	if !node.IsString() {
		return false
	}

	timeVal, err := time.Parse(n.format, node.AsString())
	if err != nil {
		return false
	}

	lhs := int(timeVal.UnixNano())

	rhs := 0
	switch n.cmpValChMode {
	case cmpValChModeNow:
		rhs = int(n.varCmpValue.Load())
	case cmpValChModeConst:
		rhs = int(n.constCmpValue)
	default:
		panic(fmt.Sprintf("impossible: invalid cmp value changing mode: %d", n.cmpValChMode))
	}

	return n.cmpOp.compare(lhs, rhs)
}

func (n *tsCmpOpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*tsCmpOpNode)
	if !ok {
		return errors.New("nodes have different types; expected: tsCmpOpNode")
	}

	if n.format != n2Explicit.format {
		return fmt.Errorf("nodes have different time formats: %s != %s", n.format, n2Explicit.format)
	}

	if n.cmpOp != n2Explicit.cmpOp {
		return fmt.Errorf("nodes have different cmp ops: %s != %s", n.cmpOp, n2Explicit.cmpOp)
	}

	if n.cmpValChMode != n2Explicit.cmpValChMode {
		return fmt.Errorf("nodes have different cmp modes: %d != %d", n.cmpValChMode, n2Explicit.cmpValChMode)
	}

	if n.constCmpValue != n2Explicit.constCmpValue {
		return fmt.Errorf(
			"nodes have different cmp values: %s != %s",
			time.Unix(0, n.constCmpValue).Format(time.RFC3339Nano),
			time.Unix(0, n2Explicit.constCmpValue).Format(time.RFC3339Nano),
		)
	}

	if n.updateInterval != n2Explicit.updateInterval {
		return fmt.Errorf(
			"nodes have different update intervals: %v != %v",
			n.updateInterval, n2Explicit.updateInterval,
		)
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr; expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
