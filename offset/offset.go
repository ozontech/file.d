package offset

import (
	"io"
	"os"
)

type LoadSaver interface {
	Load(io.Reader) error
	Save(io.Writer) error
}

type Offset struct {
	Callback LoadSaver

	path string
}

func NewOffset(path string) *Offset {
	return &Offset{path: path}
}

func (o *Offset) Load() error {
	file, err := os.Open(o.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	if err := o.Callback.Load(file); err != nil {
		return err
	}
	return nil
}

func (o *Offset) getTmpPath() string {
	return o.path + ".tmp"
}

func (o *Offset) saveToTmp() error {
	file, err := os.Create(o.getTmpPath())
	if err != nil {
		return err
	}
	defer file.Close()
	if err := o.Callback.Save(file); err != nil {
		return err
	}

	return nil
}

func (o *Offset) Save() error {
	if err := o.saveToTmp(); err != nil {
		return err
	}
	if err := os.Rename(o.getTmpPath(), o.path); err != nil {
		return err
	}

	return nil
}
