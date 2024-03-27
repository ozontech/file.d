package substitution

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"go.uber.org/zap"
)

type SubstitutionOpKind int

const (
	SubstitutionOpKindRaw SubstitutionOpKind = iota
	SubstitutionOpKindField
)

type quotesOpenStatus int

const (
	quotesOpenedNo quotesOpenStatus = iota
	quotesOpenedSingle
	quotesOpenedDouble
)

var errInvalidFilter = errors.New("invalid filter")

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
			path := cfg.ParseFieldSelector(selector)
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
