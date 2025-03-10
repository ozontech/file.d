package keep_fields

import (
	insaneJSON "github.com/ozontech/insane-json"
)

type arrayChecker struct {
	paths       [][]string
	nodePresent []bool
}

func newArrayChecker(paths [][]string) *arrayChecker {
	return &arrayChecker{
		paths:       paths,
		nodePresent: make([]bool, len(paths)),
	}
}

func (c *arrayChecker) startChecks(root *insaneJSON.Root) {
	for i := range c.nodePresent {
		c.nodePresent[i] = root.Dig(c.paths[i]...) != nil
	}
}

func (c *arrayChecker) finishChecks() {
}

func (c *arrayChecker) check(path []string) nodeStatus {
	for i, curPath := range c.paths {
		if !c.nodePresent[i] {
			continue
		}

		switch maxCommonPrefixLength(path, curPath) {
		case 0:
			break
		case len(curPath):
			return saved
		default:
			return parentOfSaved
		}
	}

	return unsaved
}

func maxCommonPrefixLength(a, b []string) int {
	n := min(len(a), len(b))

	i := 0
	for ; i < n; i++ {
		if a[i] != b[i] {
			break
		}
	}

	return i
}
