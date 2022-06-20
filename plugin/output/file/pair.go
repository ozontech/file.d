package file

// pair represents pair of given ints
type pair struct {
	min, max int64
}

// NewPair creates float pair
func NewPair() *pair {
	return &pair{}
}

// UpdatePair compares and replaces min and max with candidates
func (p *pair) UpdatePair(candidates ...int64) {
	for _, candidate := range candidates {
		if p.min == 0 || candidate < p.min {
			p.min = candidate
		}
		if candidate > p.max {
			p.max = candidate
		}
	}
}

// Reset returns min, max and resets
func (p *pair) Reset() (min int64, max int64) {
	min, max = p.min, p.max
	p.min, p.max = 0, 0
	return min, max
}

// Get returns current pair values
func (p *pair) Get() (min int64, max int64) {
	return p.min, p.max
}
