package checker

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
)

type op int

const (
	opEqual op = iota
	opContains
	opPrefix
	opSuffix
	opRegex
)

func opToString(op op) string {
	switch op {
	case opEqual:
		return "equal"
	case opContains:
		return "contains"
	case opPrefix:
		return "prefix"
	case opSuffix:
		return "suffix"
	case opRegex:
		return "regex"
	default:
		return "unknown"
	}
}

const (
	OpEqualTag    = "equal"
	OpContainsTag = "contains"
	OpPrefixTag   = "prefix"
	OpSuffixTag   = "suffix"
	OpRegexTag    = "regex"
)

func stringToOp(s string) (op, error) {
	switch s {
	case OpEqualTag:
		return opEqual, nil
	case OpContainsTag:
		return opContains, nil
	case OpPrefixTag:
		return opPrefix, nil
	case OpSuffixTag:
		return opSuffix, nil
	case OpRegexTag:
		return opRegex, nil
	default:
		return -1, fmt.Errorf("unknown field op %q", s)
	}
}

type Checker struct {
	op            op
	caseSensitive bool
	values        [][]byte
	valuesBySize  map[int][][]byte
	reValues      []*regexp.Regexp

	minValLen int
	maxValLen int
}

func New(opTag string, caseSensitive bool, values [][]byte) (*Checker, error) {
	if len(values) == 0 {
		return nil, errors.New("values are not provided")
	}

	var vals [][]byte
	var valsBySize map[int][][]byte
	var reValues []*regexp.Regexp
	var minValLen, maxValLen int

	curOp, err := stringToOp(opTag)
	if err != nil {
		return nil, err
	}

	if curOp == opRegex {
		reValues = make([]*regexp.Regexp, 0, len(values))
		for _, v := range values {
			re, err := regexp.Compile(string(v))
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex %q: %w", v, err)
			}
			reValues = append(reValues, re)
		}
	} else {
		minValLen = len(values[0])
		maxValLen = len(values[0])
		if curOp == opEqual {
			valsBySize = make(map[int][][]byte)
		} else {
			vals = make([][]byte, len(values))
		}
		for i := range values {
			var curVal []byte
			if values[i] != nil {
				curVal = make([]byte, len(values[i]))
				copy(curVal, values[i])
			}
			if !caseSensitive && curVal != nil {
				curVal = bytes.ToLower(curVal)
			}
			if len(values[i]) < minValLen {
				minValLen = len(values[i])
			}
			if len(values[i]) > maxValLen {
				maxValLen = len(values[i])
			}
			if curOp == opEqual {
				valsBySize[len(curVal)] = append(valsBySize[len(curVal)], curVal)
			} else {
				vals[i] = curVal
			}
		}
	}

	return &Checker{
		op:            curOp,
		caseSensitive: caseSensitive,
		values:        vals,
		valuesBySize:  valsBySize,
		reValues:      reValues,
		minValLen:     minValLen,
		maxValLen:     maxValLen,
	}, nil
}

func (n *Checker) Check(data []byte) bool {
	// fast check for data
	if n.op != opRegex && len(data) < n.minValLen {
		return false
	}

	switch n.op {
	case opEqual:
		vals, ok := n.valuesBySize[len(data)]
		if !ok {
			return false
		}
		if !n.caseSensitive && data != nil {
			data = bytes.ToLower(data)
		}
		for _, val := range vals {
			// null and empty strings are considered as different values
			// null can also come if field value is absent
			if (data == nil && val != nil) || (data != nil && val == nil) {
				continue
			}
			if bytes.Equal(data, val) {
				return true
			}
		}
	case opContains:
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.Contains(data, val) {
				return true
			}
		}
	case opPrefix:
		// check only necessary amount of bytes
		if len(data) > n.maxValLen {
			data = data[:n.maxValLen]
		}
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.HasPrefix(data, val) {
				return true
			}
		}
	case opSuffix:
		// check only necessary amount of bytes
		if len(data) > n.maxValLen {
			data = data[len(data)-n.maxValLen:]
		}
		if !n.caseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.values {
			if bytes.HasSuffix(data, val) {
				return true
			}
		}
	case opRegex:
		for _, re := range n.reValues {
			if re.Match(data) {
				return true
			}
		}
	}
	return false
}
