package substitution

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

const (
	regexFilterPrefix  = "re("
	trimFilterPrefix   = "trim("
	trimToFilterPrefix = "trim_to("

	bufInitSize = 1024
)

type FieldFilter interface {
	// Apply accepts src and dst slices of bytes and returns result stored in modified src slice.
	// src slice is needed to avoid unnecessary allocations.
	Apply(src []byte, dst []byte) []byte

	setBuffer([]byte)
	compareArgs([]any) error
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

// parseFilter parses filter data from string with filter args if present in format "<filter-name>(<arg1>, <arg2>, ...)".
func parseFilter(data string, logger *zap.Logger) (FieldFilter, int, error) {
	origDataLen := len(data)
	data = strings.TrimLeft(data, " ")
	offset := origDataLen - len(data)
	switch {
	case strings.HasPrefix(data, regexFilterPrefix):
		return parseRegexFilter(data, offset, logger)
	case strings.HasPrefix(data, trimFilterPrefix):
		return parseTrimFilter(data, offset)
	case strings.HasPrefix(data, trimToFilterPrefix):
		return parseTrimToFilter(data, offset)
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
