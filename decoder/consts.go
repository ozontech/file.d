package decoder

const (
	NO Type = iota
	AUTO
	JSON
	RAW
	CRI
	POSTGRES
)

type Type int
