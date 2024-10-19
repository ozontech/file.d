package decoder

import insaneJSON "github.com/ozontech/insane-json"

type Decoder interface {
	Type() Type
	Decode(root *insaneJSON.Root, data []byte) error
}
