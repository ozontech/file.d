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
	switch {
	case c.isSaved(path):
		return saved
	case c.isParentOfSaved(path):
		return parentOfSaved
	default:
		return unsaved
	}
}

func (c *arrayChecker) isSaved(path []string) bool {
	for i, curPath := range c.paths {
		if c.nodePresent[i] && equal(path, curPath) {
			return true
		}
	}

	return false
}

func (c *arrayChecker) isParentOfSaved(path []string) bool {
	for i, curPath := range c.paths {
		if !c.nodePresent[i] {
			continue
		}

		if !(len(path) < len(curPath)) {
			continue
		}

		if equal(path, curPath[:len(path)]) {
			return true
		}
	}

	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, s := range a {
		if s != b[i] {
			return false
		}
	}

	return true
}
