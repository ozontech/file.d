package substitution

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type TrimToFilter struct {
	mode   trimMode
	cutset []byte
}

func (f *TrimToFilter) Apply(src []byte, _ []byte) []byte {
	if f.mode == trimModeAll || f.mode == trimModeLeft {
		if idx := bytes.Index(src, f.cutset); idx != -1 {
			src = src[idx:]
		}
	}
	if f.mode == trimModeAll || f.mode == trimModeRight {
		if idx := bytes.LastIndex(src, f.cutset); idx != -1 {
			src = src[:idx+1]
		}
	}
	return src
}

func (f *TrimToFilter) setBuffer(buf []byte) {}

func (f *TrimToFilter) compareArgs(args []any) error {
	wantArgsCnt := 2
	if len(args) != wantArgsCnt {
		return fmt.Errorf("wrong trim filter amount of args, want=%d got=%d", wantArgsCnt, len(args))
	}
	wantMode := args[0].(trimMode)
	gotMode := f.mode
	if wantMode != gotMode {
		return fmt.Errorf("wrong trim filter mode, want=%v got=%v", wantMode, gotMode)
	}
	wantCutset := args[1].(string)
	gotCutset := string(f.cutset)
	if wantCutset != gotCutset {
		return fmt.Errorf("wrong trim filter cutset, want=%q got=%q", wantCutset, gotCutset)
	}
	return nil
}

func parseTrimToFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCnt := 2
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(trimToFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(trimToFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for trim_to filter, exptected %d, got %d", expArgsCnt, len(args))
	}
	var (
		mode    trimMode
		modeStr string
		cutset  string
	)
	if err := json.Unmarshal([]byte(args[0]), &modeStr); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse trim_to filter mode: %w", err)
	}
	mode, err = trimModeFromString(modeStr)
	if err != nil {
		return nil, filterEndPos, err
	}
	if err := json.Unmarshal([]byte(args[1]), &cutset); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse trim_to filter cutset: %w", err)
	}
	filter := &TrimToFilter{
		mode:   mode,
		cutset: []byte(cutset),
	}
	return filter, filterEndPos, nil
}
