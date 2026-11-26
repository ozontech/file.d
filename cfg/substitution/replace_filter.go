package substitution

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ReplaceFilter struct {
	old   []byte
	new   []byte
	count int
}

func (f *ReplaceFilter) Apply(src []byte, _ []byte) []byte {
	return bytes.Replace(src, f.old, f.new, f.count)
}

func (f *ReplaceFilter) setBuffer(buf []byte) {}

func (f *ReplaceFilter) compareArgs(args []any) error {
	wantArgsCnt := 3
	if len(args) != wantArgsCnt {
		return fmt.Errorf("wrong replace filter amount of args, want=%d got=%d", wantArgsCnt, len(args))
	}

	wantOld := args[0].(string)
	gotOld := string(f.old)
	if wantOld != gotOld {
		return fmt.Errorf("wrong replace filter old, want=%v got=%v", wantOld, gotOld)
	}
	wantNew := args[1].(string)
	gotNew := string(f.new)
	if wantNew != gotNew {
		return fmt.Errorf("wrong replace filter new, want=%v got=%v", wantNew, gotNew)
	}
	wantCount := args[2].(int)
	gotCount := f.count
	if wantCount != gotCount {
		return fmt.Errorf("wrong replace filter count, want=%v got=%v", wantCount, gotCount)
	}
	return nil
}

func parseReplaceFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCnt := 3
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(replaceFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(replaceFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for replace filter, exptected %d, got %d", expArgsCnt, len(args))
	}
	var (
		o, n  string
		count int
	)
	if err := json.Unmarshal([]byte(args[0]), &o); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse replace filter 'old': %w", err)
	}
	if o == "" {
		return nil, filterEndPos, fmt.Errorf("replace filter 'old' must not be empty")
	}
	if err := json.Unmarshal([]byte(args[1]), &n); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse replace filter 'new': %w", err)
	}
	if err := json.Unmarshal([]byte(args[2]), &count); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse replace filter 'count': %w", err)
	}
	filter := &ReplaceFilter{
		old:   []byte(o),
		new:   []byte(n),
		count: count,
	}
	return filter, filterEndPos, nil
}
