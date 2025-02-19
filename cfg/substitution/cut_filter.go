package substitution

import (
	"encoding/json"
	"fmt"
)

type cutMode int

const (
	cutModeFirst cutMode = iota
	cutModeLast
)

func cutModeFromString(s string) (cutMode, error) {
	var mode cutMode
	switch s {
	case "first":
		mode = cutModeFirst
	case "last":
		mode = cutModeLast
	default:
		return mode, fmt.Errorf("invalid cut mode provided %q, allowed modes are \"first\", \"last\"", s)
	}
	return mode, nil
}

type CutFilter struct {
	mode  cutMode
	count int
}

func (f *CutFilter) Apply(src []byte, _ []byte) []byte {
	if len(src) < f.count {
		return src
	}

	switch f.mode {
	case cutModeFirst:
		src = src[:f.count]
	case cutModeLast:
		src = src[len(src)-f.count:]
	}
	return src
}

func (f *CutFilter) setBuffer(buf []byte) {}

func (f *CutFilter) compareArgs(args []any) error {
	wantArgsCnt := 2
	if len(args) != wantArgsCnt {
		return fmt.Errorf("wrong cut filter amount of args, want=%d got=%d", wantArgsCnt, len(args))
	}
	wantMode := args[0].(cutMode)
	gotMode := f.mode
	if wantMode != gotMode {
		return fmt.Errorf("wrong cut filter mode, want=%v got=%v", wantMode, gotMode)
	}
	wantCount := args[1].(int)
	gotCount := f.count
	if wantCount != gotCount {
		return fmt.Errorf("wrong cut filter count, want=%q got=%q", wantCount, gotCount)
	}
	return nil
}

func parseCutFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCnt := 2
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(cutFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(cutFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for cut filter, exptected %d, got %d", expArgsCnt, len(args))
	}
	var (
		mode    cutMode
		modeStr string
		count   int
	)
	if err := json.Unmarshal([]byte(args[0]), &modeStr); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse cut filter mode: %w", err)
	}
	mode, err = cutModeFromString(modeStr)
	if err != nil {
		return nil, filterEndPos, err
	}
	if err := json.Unmarshal([]byte(args[1]), &count); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse cut filter count: %w", err)
	}
	filter := &CutFilter{
		mode:  mode,
		count: count,
	}
	return filter, filterEndPos, nil
}
