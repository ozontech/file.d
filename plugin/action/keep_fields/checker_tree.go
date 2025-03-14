package keep_fields

import insaneJSON "github.com/ozontech/insane-json"

type treeNode struct {
	s        string
	active   bool
	children []*treeNode
}

type treeChecker struct {
	root *treeNode

	paths       [][]string
	nodePresent []bool
}

func newPrefixTree(paths [][]string) *treeChecker {
	root := &treeNode{}

	for _, path := range paths {
		add(root, path)
	}

	return &treeChecker{
		root:        root,
		paths:       paths,
		nodePresent: make([]bool, len(paths)),
	}
}

// add comment
func add(root *treeNode, path []string) {
	cur := root

	for _, s := range path {
		next := (*treeNode)(nil)
		for _, child := range cur.children {
			if child.s == s {
				next = child
				break
			}
		}

		if next == nil {
			next = &treeNode{s: s}
			cur.children = append(cur.children, next)
		}

		cur = next
	}
}

func findChild(cur *treeNode, s string) *treeNode {
	for _, child := range cur.children {
		if child.s == s {
			return child
		}
	}

	return nil
}

func (t *treeChecker) startChecking(root *insaneJSON.Root) {
	for i := range t.nodePresent {
		t.nodePresent[i] = root.Dig(t.paths[i]...) != nil
	}

	for i := range t.nodePresent {
		if t.nodePresent[i] {
			t.startCheckingPath(t.paths[i])
		}
	}
}

func (t *treeChecker) startCheckingPath(path []string) {
	cur := t.root
	for _, s := range path {
		cur = findChild(cur, s)
		if cur == nil {
			panic("tree must contain this path")
		}
		cur.active = true
	}
}

func (t *treeChecker) finishChecking() {
	for i := range t.nodePresent {
		if t.nodePresent[i] {
			t.finishCheckingPath(t.paths[i])
		}
	}
}

func (t *treeChecker) finishCheckingPath(path []string) {
	cur := t.root
	for _, s := range path {
		cur = findChild(cur, s)
		if cur == nil {
			panic("tree must contain this path")
		}
		cur.active = false
	}
}

func (t *treeChecker) check(path []string) nodeStatus {
	cur := t.root
	for _, s := range path {
		if cur.children == nil {
			return unsaved
		}

		found := false
		for _, child := range cur.children {
			if child.active && child.s == s {
				cur = child
				found = true
				break
			}
		}

		if !found {
			return unsaved
		}
	}

	if cur.children == nil {
		return saved
	} else {
		return parentOfSaved
	}
}
