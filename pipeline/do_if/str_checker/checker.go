package str_checker

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
)

type op int

const (
	OpEqual op = iota
	OpContains
	OpPrefix
	OpSuffix
	OpRegex
)

func (op op) String() string {
	switch op {
	case OpEqual:
		return OpEqualTag
	case OpContains:
		return OpContainsTag
	case OpPrefix:
		return OpPrefixTag
	case OpSuffix:
		return OpSuffixTag
	case OpRegex:
		return OpRegexTag
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
		return OpEqual, nil
	case OpContainsTag:
		return OpContains, nil
	case OpPrefixTag:
		return OpPrefix, nil
	case OpSuffixTag:
		return OpSuffix, nil
	case OpRegexTag:
		return OpRegex, nil
	default:
		return -1, fmt.Errorf("unknown string op %q", s)
	}
}

type DataChecker struct {
	Op            op
	CaseSensitive bool
	Values        [][]byte
	ValuesBySize  map[int][][]byte
	ReValues      []*regexp.Regexp

	MinValLen int
	MaxValLen int
}

func New(opTag string, caseSensitive bool, values [][]byte) (DataChecker, error) {
	var def DataChecker

	if len(values) == 0 {
		return def, errors.New("values are not provided")
	}

	var vals [][]byte
	var valsBySize map[int][][]byte
	var reValues []*regexp.Regexp
	var minValLen, maxValLen int

	curOp, err := stringToOp(opTag)
	if err != nil {
		return def, err
	}

	if curOp == OpRegex {
		reValues = make([]*regexp.Regexp, 0, len(values))
		for _, v := range values {
			re, err := regexp.Compile(string(v))
			if err != nil {
				return def, fmt.Errorf("failed to compile regex %q: %w", v, err)
			}
			reValues = append(reValues, re)
		}
	} else {
		minValLen = len(values[0])
		maxValLen = len(values[0])
		if curOp == OpEqual {
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
			if curOp == OpEqual {
				valsBySize[len(curVal)] = append(valsBySize[len(curVal)], curVal)
			} else {
				vals[i] = curVal
			}
		}
	}

	return DataChecker{
		Op:            curOp,
		CaseSensitive: caseSensitive,
		Values:        vals,
		ValuesBySize:  valsBySize,
		ReValues:      reValues,
		MinValLen:     minValLen,
		MaxValLen:     maxValLen,
	}, nil
}

func MustNew(opTag string, caseSensitive bool, values [][]byte) DataChecker {
	res, err := New(opTag, caseSensitive, values)
	if err != nil {
		panic(err)
	}

	return res
}

func (n *DataChecker) Check(data []byte) bool {
	// fast check for data
	if n.Op != OpRegex && len(data) < n.MinValLen {
		return false
	}

	switch n.Op {
	case OpEqual:
		vals, ok := n.ValuesBySize[len(data)]
		if !ok {
			return false
		}
		if !n.CaseSensitive && data != nil {
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
	case OpContains:
		if !n.CaseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.Values {
			if bytes.Contains(data, val) {
				return true
			}
		}
	case OpPrefix:
		// check only necessary amount of bytes
		if len(data) > n.MaxValLen {
			data = data[:n.MaxValLen]
		}
		if !n.CaseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.Values {
			if bytes.HasPrefix(data, val) {
				return true
			}
		}
	case OpSuffix:
		// check only necessary amount of bytes
		if len(data) > n.MaxValLen {
			data = data[len(data)-n.MaxValLen:]
		}
		if !n.CaseSensitive {
			data = bytes.ToLower(data)
		}
		for _, val := range n.Values {
			if bytes.HasSuffix(data, val) {
				return true
			}
		}
	case OpRegex:
		for _, re := range n.ReValues {
			if re.Match(data) {
				return true
			}
		}
	}
	return false
}

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEqual[T comparable](a, b T, msg string) {
	assert(a == b, fmt.Sprintf("%s: %v != %v", msg, a, b))
}

func assertEqualValues(a, b [][]byte, msg string) {
	assertEqual(len(a), len(b), fmt.Sprintf("%s: different values count", msg))
	for i := range a {
		assert(
			bytes.Equal(a[i], b[i]),
			fmt.Sprintf("%s: different values at pos %d: %s != %s",
				msg, i, a[i], b[i],
			),
		)
	}
}

func Equal(a, b *DataChecker) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(r.(string))
		}
	}()

	assertEqual(a.Op, b.Op, "different op")
	assertEqual(a.CaseSensitive, b.CaseSensitive, "different case_sensitive")
	assertEqualValues(a.Values, b.Values, "different values")

	assertEqual(len(a.ValuesBySize), len(b.ValuesBySize), "different ValuesBySize len")
	for size := range a.ValuesBySize {
		_, found := b.ValuesBySize[size]
		assert(found, fmt.Sprintf("not found values by size %d", size))
		assertEqualValues(
			a.ValuesBySize[size], b.ValuesBySize[size],
			fmt.Sprintf("different values by size %d", size),
		)
	}

	assertEqual(len(a.ReValues), len(b.ReValues), "different regex values count")
	for i := range a.ReValues {
		assertEqual(
			a.ReValues[i].String(), b.ReValues[i].String(),
			fmt.Sprintf("different regex values at pos %d", i),
		)
	}

	assertEqual(a.MinValLen, b.MinValLen, "different min value len")
	assertEqual(a.MaxValLen, b.MaxValLen, "different max value len")

	return nil
}
