package substitution

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type TrimToFilter struct {
	mode          trimMode
	cutset        []byte
	includeCutset bool
}

func (f *TrimToFilter) Apply(src []byte, _ []byte) []byte {
	if f.mode == trimModeAll || f.mode == trimModeLeft {
		if idx := bytes.Index(src, f.cutset); idx != -1 {
			if f.includeCutset {
				idx += len(f.cutset)
			}
			src = src[idx:]
		}
	}
	if f.mode == trimModeAll || f.mode == trimModeRight {
		if idx := bytes.LastIndex(src, f.cutset); idx != -1 {
			if f.includeCutset {
				src = src[:idx]
			} else {
				src = src[:idx+len(f.cutset)]
			}
		}
	}
	return src
}

func (f *TrimToFilter) setBuffer(buf []byte) {}

func (f *TrimToFilter) compareArgs(args []any) error {
	wantArgsCntMin := 2
	wantArgsCntMax := 3
	if len(args) < wantArgsCntMin || len(args) > wantArgsCntMax {
		return fmt.Errorf("wrong trim_to filter amount of args, want=[%d:%d] got=%d", wantArgsCntMin, wantArgsCntMax, len(args))
	}

	wantMode := args[0].(trimMode)
	gotMode := f.mode
	if wantMode != gotMode {
		return fmt.Errorf("wrong trim_to filter mode, want=%v got=%v", wantMode, gotMode)
	}
	wantCutset := args[1].(string)
	gotCutset := string(f.cutset)
	if wantCutset != gotCutset {
		return fmt.Errorf("wrong trim_to filter cutset, want=%q got=%q", wantCutset, gotCutset)
	}
	if len(args) > wantArgsCntMin {
		wantIncludeCutset := args[2].(bool)
		gotIncludeCutset := f.includeCutset
		if wantIncludeCutset != gotIncludeCutset {
			return fmt.Errorf("wrong trim_to filter flag 'includeCutset', want=%v got=%v", wantIncludeCutset, gotIncludeCutset)
		}
	}
	return nil
}

func parseTrimToFilter(data string, offset int) (FieldFilter, int, error) {
	expArgsCntMin := 2
	expArgsCntMax := 3
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(trimToFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(trimToFilterPrefix) + offset
	if len(args) < expArgsCntMin || len(args) > expArgsCntMax {
		return nil, filterEndPos, fmt.Errorf("invalid args count for trim_to filter, exptected from %d to %d, got %d", expArgsCntMin, expArgsCntMax, len(args))
	}
	var (
		mode          trimMode
		modeStr       string
		cutset        string
		includeCutset bool
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
	if cutset == "" {
		return nil, filterEndPos, fmt.Errorf("trim_to filter cutset must not be empty")
	}
	if len(args) > expArgsCntMin {
		if err := json.Unmarshal([]byte(args[2]), &includeCutset); err != nil {
			return nil, filterEndPos, fmt.Errorf("failed to parse trim_to filter flag 'includeCutset': %w", err)
		}
	}
	filter := &TrimToFilter{
		mode:          mode,
		cutset:        []byte(cutset),
		includeCutset: includeCutset,
	}
	return filter, filterEndPos, nil
}
