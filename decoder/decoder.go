package decoder

import insaneJSON "github.com/ozontech/insane-json"

type Type int

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

type Decoder interface {
	Type() Type
	DecodeToJson(root *insaneJSON.Root, data []byte) error
	Decode(data []byte, args ...any) (any, error)
}
