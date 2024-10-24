package decoder

import insaneJSON "github.com/vitkovskii/insane-json"

func DecodeJson(event *insaneJSON.Root, data []byte) error {
	return event.DecodeBytes(data)
}

func DecodeJsonTo(event *insaneJSON.Root, data []byte) (*insaneJSON.Node, error) {
	return event.DecodeBytesAdditional(data)
}
