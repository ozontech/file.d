package doif

import insaneJSON "github.com/ozontech/insane-json"

type rootData struct {
	root *insaneJSON.Root
}

func NewRootData(root *insaneJSON.Root) rootData {
	return rootData{
		root: root,
	}
}

func (d rootData) Get(path ...string) []byte {
	var data []byte
	if d.root == nil {
		return nil
	}
	node := d.root.Dig(path...)
	if node.IsArray() || node.IsObject() {
		return make([]byte, 1)
	}
	if !node.IsNull() {
		data = node.AsBytes()
	}
	return data
}
