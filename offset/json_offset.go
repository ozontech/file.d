package offset

import (
	"encoding/json"
	"io"
)

type jsonValue struct {
	value interface{}
}

func (o *jsonValue) Load(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, o.value)
}

func (o *jsonValue) Save(w io.Writer) error {
	b, err := json.Marshal(o.value)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
	if err != nil {
		return err
	}
	return nil
}

func newJsonOffset(path string, value interface{}) *Offset {
	res := NewOffset(path)
	res.Callback = &jsonValue{value}
	return res
}

func LoadJson(path string, value interface{}) error {
	return newJsonOffset(path, value).Load()
}

func SaveJson(path string, value interface{}) error {
	return newJsonOffset(path, value).Save()
}
