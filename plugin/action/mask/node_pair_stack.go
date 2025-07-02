package mask

import insaneJSON "github.com/ozontech/insane-json"

type nodePair struct {
	jNode  *insaneJSON.Node
	fmNode *fieldMasksNode
}

type stack struct {
	arr []nodePair
}

func newStack(elems ...nodePair) stack {
	return stack{arr: append(make([]nodePair, 0), elems...)}
}

func (s *stack) add(n nodePair) {
	s.arr = append(s.arr, n)
}

func (s *stack) pop() nodePair {
	if len(s.arr) == 0 {
		return nodePair{}
	}
	last := s.arr[len(s.arr)-1]
	s.arr = s.arr[:len(s.arr)-1]
	return last
}

func (s *stack) isEmpty() bool {
	return len(s.arr) == 0
}
