package file

// pair represents pair of given uints.
type pair struct {
	least   float64
	largest float64
}

// NewPair creates pair to timestamps.
func NewPair(first, last float64) *pair {
	return &pair{
		least:   first,
		largest: last,
	}
}

// UpdatePair compares ts with first and last members, trying to replace them.
// It's not concurrent save.
func (p *pair) UpdatePair(candidates ...float64) {
	currLeast := p.least
	currLargest := p.largest

	for _, candidate := range candidates {
		if currLeast == 0 || candidate < currLeast {
			currLeast = candidate
		}
		if candidate > currLargest {
			currLargest = candidate
		}
	}

	if p.least == 0 || currLeast < p.least {
		p.least = currLeast
	}
	if currLargest > p.largest {
		p.largest = currLargest
	}
}

// Reset returns pair values and resets.
func (p *pair) Reset() (float64, float64) {
	least, largest := p.least, p.largest
	p.least, p.largest = 0, 0
	return least, largest
}

func (p *pair) Get() (float64, float64) {
	return p.least, p.largest
}
