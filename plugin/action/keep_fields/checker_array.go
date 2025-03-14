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
	isParentOfSaved := false

	for i, curPath := range c.paths {
		if !c.nodePresent[i] {
			continue
		}

		if !(len(path) <= len(curPath)) {
			continue
		}

		isPrefix := equal(path, curPath[:len(path)])
		if isPrefix {
			if len(path) == len(curPath) {
				return saved
			} else {
				isParentOfSaved = true
			}
		}
	}

	if isParentOfSaved {
		return parentOfSaved
	} else {
		return unsaved
	}
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
