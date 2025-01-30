package json_extract

type pathNode struct {
	data     string
	children pathNodes
}

type pathNodes []*pathNode

func (pn pathNodes) find(data string) *pathNode {
	for _, n := range pn {
		if n.data == data {
			return n
		}
	}
	return nil
}

// pathTree is multi linked list.
//
// For example, we have list of paths:
//
//   - f1.f2.f3
//   - f1.f4
//   - f1.f2.f5
//   - f6
//
// After add all specified paths to [pathTree], we will get the following:
//
//   - root's children -> f1, f6
//   - f1's children -> f2, f4
//   - f2's children -> f3, f5
type pathTree struct {
	root *pathNode
}

func newPathTree() *pathTree {
	return &pathTree{
		root: &pathNode{
			children: make([]*pathNode, 0),
		},
	}
}

func (l *pathTree) add(path []string) {
	cur := l.root
	depth := 0
	for depth < len(path)-1 {
		found := false
		for _, c := range cur.children {
			if c.data == path[depth] {
				cur = c
				depth++
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	for i := depth; i < len(path); i++ {
		newNode := &pathNode{
			data:     path[i],
			children: make([]*pathNode, 0),
		}
		cur.children = append(cur.children, newNode)
		cur = newNode
	}
}
