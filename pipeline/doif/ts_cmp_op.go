package doif

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/xtime"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ do-if-ts-cmp-op-node
DoIf timestamp comparison op node is considered to always be a leaf in the DoIf tree like DoIf field op node.
It contains operation that compares timestamps with certain value.

Params:
  - `op` - must be `ts_cmp`. Required.
  - `field` - name of the field to apply operation. Required. Field will be parsed with `time.Parse` function.
  - `format` - format for timestamps representation.
Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
Optional; default = `rfc3339nano`.
  - `cmp_op` - comparison operation name (same as for length comparison operations). Required.
  - `value` - timestamp value to compare field timestamps with. It must have `RFC3339Nano` format. Required.
Also, it may be `now` or `file_d_start`. If it is `now` then value to compare timestamps with is periodically updated current time.
If it is `file_d_start` then value to compare timestamps with will be program start moment.
  - `value_shift` - duration that adds to `value` before comparison. It can be negative. Useful when `value` is `now`.
Optional; default = 0.
  - `update_interval` - if `value` is `now` then you can set update interval for that value. Optional; default = 10s.
Actual cmp value in that case is `now + value_shift + update_interval`.

Example (discard all events with `timestamp` field value LESS than `2010-01-01T00:00:00Z`):
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
	cmpValChangeModeConst cmpValueChangingMode = iota
	cmpValChangeModeNow
)

const (
	cmpValChangeModeConstTag = "const"
	cmpValChangeModeNowTag   = "now"
)

type tsCmpOpNode struct {
	fieldPath []string
	format    string

	cmpOp cmpOperation

	cmpValChangeMode cmpValueChangingMode
	constCmpValue    int64
	varCmpValue      atomic.Int64
	cmpValueShift    int64 // may be negative
	updateInterval   time.Duration
}

func NewTsCmpOpNode(field string, format string, cmpOp string, cmpValChangeMode string, cmpValue time.Time, cmpValueShift time.Duration, updateInterval time.Duration) (Node, error) {
	typedCmpOp, err := newCmpOp(cmpOp)
	if err != nil {
		return nil, err
	}

	fieldPath := cfg.ParseFieldSelector(field)

	var resCmpValChangeMode cmpValueChangingMode
	switch cmpValChangeMode {
	case cmpValChangeModeNowTag:
		resCmpValChangeMode = cmpValChangeModeNow
	case cmpValChangeModeConstTag:
		resCmpValChangeMode = cmpValChangeModeConst
	default:
		return nil, fmt.Errorf("unknown ts cmp mode: %s", cmpValChangeMode)
	}

	parsedFormat, err := xtime.ParseFormatName(format)
	if err != nil {
		parsedFormat = format
	}

	result := &tsCmpOpNode{
		fieldPath:        fieldPath,
		format:           parsedFormat,
		cmpOp:            typedCmpOp,
		cmpValChangeMode: resCmpValChangeMode,
		constCmpValue:    cmpValue.UnixNano(),
		cmpValueShift:    cmpValueShift.Nanoseconds(),
		updateInterval:   updateInterval,
	}
	result.startUpdater()

	return result, nil
}

func (n *tsCmpOpNode) startUpdater() {
	if n.cmpValChangeMode == cmpValChangeModeNow {
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

	timeVal, err := xtime.ParseTime(n.format, node.AsString())
	if err != nil {
		return false
	}

	lhs := int(timeVal.UnixNano())

	rhs := int64(0)
	switch n.cmpValChangeMode {
	case cmpValChangeModeNow:
		rhs = n.varCmpValue.Load() + n.updateInterval.Nanoseconds()
	case cmpValChangeModeConst:
		rhs = n.constCmpValue
	default:
		panic(fmt.Sprintf("impossible: invalid cmp value changing mode: %d", n.cmpValChangeMode))
	}

	rhs += n.cmpValueShift

	return n.cmpOp.compare(lhs, int(rhs))
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

	if n.cmpValChangeMode != n2Explicit.cmpValChangeMode {
		return fmt.Errorf("nodes have different cmp modes: %d != %d", n.cmpValChangeMode, n2Explicit.cmpValChangeMode)
	}

	if n.constCmpValue != n2Explicit.constCmpValue {
		return fmt.Errorf(
			"nodes have different cmp values: %s != %s",
			time.Unix(0, n.constCmpValue).Format(time.RFC3339Nano),
			time.Unix(0, n2Explicit.constCmpValue).Format(time.RFC3339Nano),
		)
	}

	if n.cmpValueShift != n2Explicit.cmpValueShift {
		return fmt.Errorf(
			"nodes have different cmp value shifts: %s != %s",
			time.Duration(n.cmpValueShift),
			time.Duration(n2Explicit.cmpValueShift),
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
