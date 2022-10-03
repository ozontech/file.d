package decoder

const (
	NO DecoderType = iota
	AUTO
	JSON
	RAW
	CRI
	POSTGRES
	NGINX_ERROR
)

type DecoderType int
