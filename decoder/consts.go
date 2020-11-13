package decoder

const (
	NO DecoderType = iota
	AUTO
	JSON
	RAW
	CRI
)

type DecoderType int
