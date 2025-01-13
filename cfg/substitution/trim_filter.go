package substitution

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type trimMode int

const (
	trimModeAll trimMode = iota
	trimModeLeft
	trimModeRight
)

func trimModeFromString(s string) (trimMode, error) {
	var mode trimMode
	switch s {
	case "all":
		mode = trimModeAll
	case "left":
		mode = trimModeLeft
	case "right":
		mode = trimModeRight
	default:
		return mode, fmt.Errorf("invalid trim mode provided %q, allowed modes are \"all\", \"left\", \"right\"", s)
	}
	return mode, nil
}

type TrimFilter struct {
	mode   trimMode
	cutset string
}

func (t *TrimFilter) Apply(src []byte, _ []byte) []byte {
	switch t.mode {
	case trimModeLeft:
		return bytes.TrimLeft(src, t.cutset)
	case trimModeRight:
		return bytes.TrimRight(src, t.cutset)
	default:
		return bytes.Trim(src, t.cutset)
	}
}

func (t *TrimFilter) setBuffer(buf []byte) {}

func (t *TrimFilter) compareArgs(args []any) error {
	wantArgsCnt := 2
	if len(args) != wantArgsCnt {
		return fmt.Errorf("wrong trim filter amount of args, want=%d got=%d", wantArgsCnt, len(args))
	}
	wantMode := args[0].(trimMode)
	gotMode := t.mode
	if wantMode != gotMode {
		return fmt.Errorf("wrong trim filter mode, want=%v got=%v", wantMode, gotMode)
	}
	wantCutset := args[1].(string)
	gotCutset := t.cutset
	if wantCutset != gotCutset {
		return fmt.Errorf("wrong trim filter cutset, want=%q got=%q", wantCutset, gotCutset)
	}
	return nil
}

func parseTrimFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCnt := 2
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(trimFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(trimFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for trim filter, exptected %d, got %d", expArgsCnt, len(args))
	}
	var mode trimMode
	var modeStr string
	var cutset string
	if err := json.Unmarshal([]byte(args[0]), &modeStr); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse trim filter mode: %w", err)
	}
	mode, err = trimModeFromString(modeStr)
	if err != nil {
		return nil, filterEndPos, err
	}
	if err := json.Unmarshal([]byte(args[1]), &cutset); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse trim filter cutset: %w", err)
	}
	filter := &TrimFilter{
		mode:   mode,
		cutset: cutset,
	}
	return filter, filterEndPos, nil
}
