package decoder

const (
	NO DecoderType = iota
	AUTO
	JSON
	RAW
	CRI
	POSTGRES
)

type DecoderType int
