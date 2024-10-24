package decoder

import insaneJSON "github.com/vitkovskii/insane-json"

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
	Decode(root *insaneJSON.Root, data []byte) error
}
