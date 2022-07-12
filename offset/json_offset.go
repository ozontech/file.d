package offset

import (
	"bytes"
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

	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()

	return d.Decode(o.value)
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

func SaveJson(path string, value interface{}) error {
	return newJsonOffset(path, value).Save()
}

func LoadJson(path string, value interface{}) error {
	return newJsonOffset(path, value).Load()
}
