package substitution

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ReplaceAllFilter struct {
	old []byte
	new []byte
}

func (f *ReplaceAllFilter) Apply(src []byte, _ []byte) []byte {
	return bytes.ReplaceAll(src, f.old, f.new)
}

func (f *ReplaceAllFilter) setBuffer(buf []byte) {}

func (f *ReplaceAllFilter) compareArgs(args []any) error {
	wantArgsCnt := 2
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
	return nil
}

func parseReplaceAllFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCnt := 2
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(replaceAllFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(replaceAllFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for replace_all filter, exptected %d, got %d", expArgsCnt, len(args))
	}

	var old, new string
	if err := json.Unmarshal([]byte(args[0]), &old); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse replace_all filter 'old': %w", err)
	}
	if len(old) == 0 {
		return nil, filterEndPos, fmt.Errorf("replace_all filter 'old' must not be empty")
	}
	if err := json.Unmarshal([]byte(args[1]), &new); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse replace_all filter 'new': %w", err)
	}
	filter := &ReplaceAllFilter{
		old: []byte(old),
		new: []byte(new),
	}
	return filter, filterEndPos, nil
}
