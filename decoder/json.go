package decoder

import insaneJSON "github.com/vitkovskii/insane-json"

func DecodeJson(root *insaneJSON.Root, data []byte) error {
	return root.DecodeBytes(data)
}

func DecodeJsonToNode(root *insaneJSON.Root, data []byte) (*insaneJSON.Node, error) {
	return root.DecodeBytesAdditional(data)
}
