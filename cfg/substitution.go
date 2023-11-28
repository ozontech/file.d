package cfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

type SubstitutionOpKind int

const (
	SubstitutionOpKindRaw SubstitutionOpKind = iota
	SubstitutionOpKindField

	regexFilterPrefix = "re("

	bufInitSize = 1024
)

type quotesOpenStatus int

const (
	quotesOpenedNo quotesOpenStatus = iota
	quotesOpenedSingle
	quotesOpenedDouble
)

var errInvalidFilter = errors.New("invalid filter")

type FieldFilter interface {
	// Apply accepts src and dst slices of bytes and returns result stored in modified src slice.
	// src slice is needed to avoid unnecessary allocations.
	Apply(src []byte, dst []byte) []byte

	setBuffer([]byte)
	compareArgs([]any) error
}

type RegexFilter struct {
	re        *regexp.Regexp
	groups    []int
	separator []byte

	buf []byte
}

func (r *RegexFilter) Apply(src []byte, dst []byte) []byte {
	if len(r.groups) == 0 {
		return dst
	}
	indexes := r.re.FindAllSubmatchIndex(src, -1)
	if len(indexes) == 0 {
		return dst
	}
	r.buf = r.buf[:0]
	for _, index := range indexes {
		for _, grp := range r.groups {
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
	if len(args) != 3 {
		return fmt.Errorf("wrong regex filter amount of args, want=%d got=%d", 3, len(args))
	}
	wantRe := args[0].(string)
	gotRe := r.re.String()
	if wantRe != gotRe {
		return fmt.Errorf("wrong regex filter regex expr, want=%q got=%q", wantRe, gotRe)
	}
	wantGroups := args[1].([]int)
	gotGroups := r.groups
	if len(wantGroups) != len(gotGroups) {
		return fmt.Errorf("wrong regex filter groups, want=%v got=%v", wantGroups, gotGroups)
	}
	for i := 0; i < len(wantGroups); i++ {
		if wantGroups[i] != gotGroups[i] {
			return fmt.Errorf("wrong regex filter groups, want=%v got=%v", wantGroups, gotGroups)
		}
	}
	wantSeparator := args[2].(string)
	gotSeparator := string(r.separator)
	if wantSeparator != gotSeparator {
		return fmt.Errorf("wrong regex filter separator, want=%q got=%q", wantSeparator, gotSeparator)
	}
	return nil
}

// indexRuneInExpr same as strings.IndexByte but depending on flags
// considers sought rune is outside quotes and whether it is escaped explicitly
// using separate escaping slash (e.g. "\\d")
func indexRuneInExpr(expr string, c rune, considerQuotes, considerEscaped bool) int {
	if !considerQuotes && considerEscaped {
		return strings.IndexByte(expr, byte(c))
	}
	quotesOpened := quotesOpenedNo
searchLoop:
	for i, b := range expr {
		if considerQuotes && (i == 0 || expr[i-1] != '\\') {
			switch {
			case quotesOpened == quotesOpenedNo:
				if b == '\'' {
					quotesOpened = quotesOpenedSingle
					continue searchLoop
				} else if b == '"' {
					quotesOpened = quotesOpenedDouble
					continue searchLoop
				}
			case quotesOpened == quotesOpenedSingle && b == '\'':
				quotesOpened = quotesOpenedNo
				continue
			case quotesOpened == quotesOpenedDouble && b == '"':
				quotesOpened = quotesOpenedNo
				continue
			}
			// when quotes are considered skip all bytes inside quotes
			if quotesOpened != quotesOpenedNo {
				continue
			}
		}
		if b == c && (considerEscaped || i == 0 || expr[i-1] != '\\') {
			return i
		}
	}
	return -1
}

type SubstitutionOp struct {
	Kind    SubstitutionOpKind
	Data    []string
	Filters []FieldFilter
}

// parseFilterOps parses a chain of field filters from substitution string
// `${field|filter1|filter2|...|filterN}` -> `<filter1>,<filter2>,...,<filterN>`.
func parseFilterOps(substitution string, pipePos, endPos int, filterBuf []byte, logger *zap.Logger) ([]FieldFilter, error) {
	var filterOps []FieldFilter
	offset := 0
	for pipePos != -1 {
		pipePos += offset
		filterOp, filterEndPos, err := parseFilter(substitution[pipePos+1:endPos], logger)
		if err != nil {
			return nil, err
		}
		// single buffer for all filters because there is only one event for a substitution op simultaneously
		// and filters in substitution op are applied sequentially one by one
		if filterBuf == nil {
			filterBuf = make([]byte, 0, bufInitSize)
		}
		filterOp.setBuffer(filterBuf)
		filterOps = append(filterOps, filterOp)
		offset = pipePos + 1 + filterEndPos
		pipePos = indexRuneInExpr(substitution[offset:endPos], '|', true, false)
	}
	return filterOps, nil
}

func ParseSubstitution(substitution string, filtersBuf []byte, logger *zap.Logger) ([]SubstitutionOp, error) {
	var err error
	result := make([]SubstitutionOp, 0)
	tail := ""
	for {
		pos := strings.IndexByte(substitution, '$')
		// `len(substitution) == pos + 1` is a corner case of a single symbol '$' at the end
		if pos == -1 || len(substitution) == pos+1 {
			break
		}

		if len(substitution) < pos+1 {
			substitution = substitution[pos+1:]
			continue
		}

		switch substitution[pos+1] {
		case '$':
			tail = substitution[:pos+1]
			substitution = substitution[pos+2:]
		case '{':
			// append SubstitutionOpKindRaw only if there is non-empty content
			if len(tail)+len(substitution[:pos]) > 0 {
				result = append(result, SubstitutionOp{
					Kind: SubstitutionOpKindRaw,
					Data: []string{tail + substitution[:pos]},
				})
				tail = ""
			}

			end := indexRuneInExpr(substitution, '}', true, false)
			if end == -1 {
				return nil, fmt.Errorf("can't find substitution end '}': %s", substitution)
			}

			var filterOps []FieldFilter
			selectorEnd := end
			pipe := indexRuneInExpr(substitution, '|', true, false)
			if pipe != -1 {
				selectorEnd = pipe
				filterOps, err = parseFilterOps(substitution, pipe, end, filtersBuf, logger)
				if err != nil {
					return nil, err
				}
			}

			selector := substitution[pos+2 : selectorEnd]
			path := ParseFieldSelector(selector)
			result = append(result, SubstitutionOp{
				Kind:    SubstitutionOpKindField,
				Data:    path,
				Filters: filterOps,
			})

			substitution = substitution[end+1:]
		default:
			tail = substitution[:pos+1]
			substitution = substitution[pos+1:]
		}
	}

	if len(substitution)+len(tail) != 0 {
		result = append(result, SubstitutionOp{
			Kind: SubstitutionOpKindRaw,
			Data: []string{tail + substitution},
		})
	}

	return result, nil
}

func parseRegexFilter(data string, offset int, logger *zap.Logger) (FieldFilter, int, error) {
	filterEndPos := -1
	args, argsEndPos, err := parseFilterArgs(data[len(regexFilterPrefix):])
	if err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse filter args: %w", err)
	}
	filterEndPos = argsEndPos + len(regexFilterPrefix) + offset
	if len(args) != 3 {
		return nil, filterEndPos, fmt.Errorf("invalid args for regexp filter, exptected 3, got %d", len(args))
	}
	var reStr string
	var groups []int
	var separator string
	if err := json.Unmarshal([]byte(args[0]), &reStr); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter regexp string: %w", err)
	}
	re := regexp.MustCompile(reStr)
	if err := json.Unmarshal([]byte(args[1]), &groups); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter groups: %w", err)
	}
	VerifyGroupNumbers(groups, re.NumSubexp(), logger)
	if err := json.Unmarshal([]byte(args[2]), &separator); err != nil {
		return nil, filterEndPos, fmt.Errorf("failed to parse regexp filter separator: %w", err)
	}
	filter := &RegexFilter{
		re:        re,
		groups:    groups,
		separator: []byte(separator),
	}
	return filter, filterEndPos, nil
}

// parseFilter parses filter data from string with filter args if present in format "<filter-name>(<arg1>, <arg2>, ...)".
func parseFilter(data string, logger *zap.Logger) (FieldFilter, int, error) {
	origDataLen := len(data)
	data = strings.TrimLeft(data, " ")
	offset := origDataLen - len(data)
	if strings.HasPrefix(data, regexFilterPrefix) {
		return parseRegexFilter(data, offset, logger)
	}
	return nil, -1, errInvalidFilter
}

// parseFilterArgs parses args from string in format of "<arg1>, <arg2>, ...)" --
// filter args without its name and starting bracket.
func parseFilterArgs(data string) ([]string, int, error) {
	var args []string
	// no args
	if data[0] == ')' {
		return args, 0, nil
	}
	argsEndPos := -1
	quotesOpened := quotesOpenedNo
	brackets := make([]byte, 0, 100)
	curArg := make([]byte, 0, 100)
	// parse args separated by comma ','
	// re("((),()),()", [1, 2, 3], "'(,)'")
gatherLoop:
	for i, b := range []byte(data) {
		// escaped characters handling
		if i != 0 && data[i-1] == '\\' {
			curArg = append(curArg, b)
			continue
		}
		// quotes have a priority over brackets
		switch {
		case quotesOpened == quotesOpenedNo:
			switch {
			case b == '\'':
				quotesOpened = quotesOpenedSingle
			case b == '"':
				quotesOpened = quotesOpenedDouble
			case b == '(' || b == '[' || b == '{':
				brackets = append(brackets, b)
			case b == ')' && len(brackets) == 0:
				// early stop on the end of args list
				argsEndPos = i
				break gatherLoop
			case b == ')' || b == ']':
				lastBracket := brackets[len(brackets)-1]
				switch {
				case b == ')' && lastBracket != '(':
					return nil, argsEndPos, fmt.Errorf("invalid brackets: %s", data)
				case b == ']' && lastBracket != '[':
					return nil, argsEndPos, fmt.Errorf("invalid brackets: %s", data)
				}
				brackets = brackets[:len(brackets)-1]
			case b == ',' && len(brackets) == 0:
				// condition for separating args
				args = append(args, string(curArg))
				curArg = curArg[:0]
				continue gatherLoop
			}
		case b == '\'' && quotesOpened == quotesOpenedSingle:
			quotesOpened = quotesOpenedNo
		case b == '"' && quotesOpened == quotesOpenedDouble:
			quotesOpened = quotesOpenedNo
		}
		curArg = append(curArg, b)
	}
	if len(brackets) > 0 {
		return nil, argsEndPos, fmt.Errorf("invalid brackets: %s", data)
	}
	if quotesOpened != quotesOpenedNo {
		return nil, argsEndPos, fmt.Errorf("not all quotes are closed: %s", data)
	}
	if argsEndPos == -1 {
		return nil, argsEndPos, fmt.Errorf("no closing bracket for args list: %s", data)
	}
	// last arg
	if len(curArg) > 0 {
		args = append(args, string(curArg))
	}
	for i := range args {
		args[i] = strings.TrimSpace(args[i])
	}
	return args, argsEndPos, nil
}
