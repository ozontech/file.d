package doif

import insaneJSON "github.com/ozontech/insane-json"

type eventData struct {
	root *insaneJSON.Root
}

func NewEventData(root *insaneJSON.Root) eventData {
	return eventData{
		root: root,
	}
}

func (d eventData) Get(path ...string) []byte {
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
