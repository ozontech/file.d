package cfg

import (
	"fmt"
	"strings"

	"github.com/ozontech/file.d/cfg/parse"
)

type SubstitutionOpKind int

const (
	SubstitutionOpKindRaw SubstitutionOpKind = iota
	SubstitutionOpKindField
)

type SubstitutionOp struct {
	Kind SubstitutionOpKind
	Data []string
}

func ParseSubstitution(substitution string) ([]SubstitutionOp, error) {
	result := make([]SubstitutionOp, 0)
	tail := ""
	for {
		pos := strings.IndexByte(substitution, '$')
		if pos == -1 {
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
			result = append(result, SubstitutionOp{
				Kind: SubstitutionOpKindRaw,
				Data: []string{tail + substitution[:pos]},
			})
			tail = ""

			end := strings.IndexByte(substitution, '}')
			if end == -1 {
				return nil, fmt.Errorf("can't find substitution end '}': %s", substitution)
			}

			selector := substitution[pos+2 : end]
			path := parse.ParseFieldSelector(selector)
			result = append(result, SubstitutionOp{
				Kind: SubstitutionOpKindField,
				Data: path,
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
