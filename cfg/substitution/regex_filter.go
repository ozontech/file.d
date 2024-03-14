package substitution

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/ozontech/file.d/cfg"
	"go.uber.org/zap"
)

type RegexFilter struct {
	re        *regexp.Regexp
	limit     int
	groups    []int
	separator []byte

	buf []byte
}

func (r *RegexFilter) Apply(src []byte, dst []byte) []byte {
	if len(r.groups) == 0 {
		return dst
	}
	indexes := r.re.FindAllSubmatchIndex(src, r.limit)
	if len(indexes) == 0 {
		return dst
	}
	r.buf = r.buf[:0]
	for _, index := range indexes {
		for _, grp := range r.groups {
			// (*regexp.Regexp).FindAllSubmatchIndex(...) returns a slice of indexes in format:
			// [<start of whole regexp match>, <end of whole regexp match>, <start of group 1>, <end of group 1>, ...].
			// So if there are more than one group in regexp the first two indexes must be skipped.
			// Start of group 1 is index[2], end of group 1 is index[3], start of group 2 is index[4], end of group 2 is index[5],
			// and so on. Hence, the start of group i is index[2*i], the end of group i is index[2*i+1].
			start := index[grp*2]
			end := index[grp*2+1]
			if len(r.separator) > 0 && len(r.buf) != 0 {
				r.buf = append(r.buf, r.separator...)
			}
			r.buf = append(r.buf, src[start:end]...)
		}
	}
	if cap(dst) < len(r.buf) {
		dst = make([]byte, len(r.buf))
	} else {
		dst = dst[:len(r.buf)]
	}
	copy(dst, r.buf)
	return dst
}

func (r *RegexFilter) setBuffer(buf []byte) {
	r.buf = buf
}

// compareArgs is used for testing. Checks filter args values.
func (r *RegexFilter) compareArgs(args []any) error {
	wantArgsCnt := 4
	if len(args) != wantArgsCnt {
		return fmt.Errorf("wrong regex filter amount of args, want=%d got=%d", wantArgsCnt, len(args))
	}
	wantRe := args[0].(string)
	gotRe := r.re.String()
	if wantRe != gotRe {
		return fmt.Errorf("wrong regex filter regex expr, want=%q got=%q", wantRe, gotRe)
	}
	wantLimit := args[1].(int)
	gotLimit := r.limit
	if wantLimit != gotLimit {
		return fmt.Errorf("wrong regex filter limit, want=%v got=%v", wantLimit, gotLimit)
	}
	wantGroups := args[2].([]int)
	gotGroups := r.groups
	if len(wantGroups) != len(gotGroups) {
		return fmt.Errorf("wrong regex filter groups, want=%v got=%v", wantGroups, gotGroups)
	}
	for i := 0; i < len(wantGroups); i++ {
		if wantGroups[i] != gotGroups[i] {
			return fmt.Errorf("wrong regex filter groups, want=%v got=%v", wantGroups, gotGroups)
		}
	}
	wantSeparator := args[3].(string)
	gotSeparator := string(r.separator)
	if wantSeparator != gotSeparator {
		return fmt.Errorf("wrong regex filter separator, want=%q got=%q", wantSeparator, gotSeparator)
	}
	return nil
}

func parseRegexFilter(data string, offset int, logger *zap.Logger) (FieldFilter, int, error) {
	expArgsCnt := 4
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(regexFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(regexFilterPrefix) + offset
	if len(args) != expArgsCnt {
		return nil, filterEndPos, fmt.Errorf("invalid args for regexp filter, exptected %d, got %d", expArgsCnt, len(args))
	}
	var reStr string
	var limit int
	var groups []int
	var separator string
	if err := json.Unmarshal([]byte(args[0]), &reStr); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter regexp string: %w", err)
	}
	re := regexp.MustCompile(reStr)
	if err := json.Unmarshal([]byte(args[1]), &limit); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter limit: %w", err)
	}
	if err := json.Unmarshal([]byte(args[2]), &groups); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter groups: %w", err)
	}
	cfg.VerifyGroupNumbers(groups, re.NumSubexp(), logger)
	if err := json.Unmarshal([]byte(args[3]), &separator); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter separator: %w", err)
	}
	filter := &RegexFilter{
		re:        re,
		limit:     limit,
		groups:    groups,
		separator: []byte(separator),
	}
	return filter, filterEndPos, nil
}
