package file

// pair represents pair of given uints.
type pair struct {
	least   uint64
	largest uint64
}

// NewPair creates pair to timestamps.
func NewPair(first, last uint64) *pair {
	return &pair{
		least:   first,
		largest: last,
	}
}

// UpdatePair compares ts with first and last members, trying to replace them.
// It's not concurrent save.
func (p *pair) UpdatePair(candidate uint64) {
	least := p.least
	if least == 0 || candidate < least {
		p.least = candidate
	}
	if candidate > p.largest {
		p.largest = candidate
	}
}

// Reset returns pair values and resets.
func (p *pair) Reset() (uint64, uint64) {
	least, largest := p.least, p.largest
	p.least, p.largest = 0, 0
	return least, largest
}
