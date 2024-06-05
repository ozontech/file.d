package decoder

const (
	NO Type = iota
	AUTO
	JSON
	RAW
	CRI
	POSTGRES
	NGINX_ERROR
	PROTOBUF
)

type Type int
