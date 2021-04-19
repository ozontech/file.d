package common

import (
	"io"
	"os"
)

type ReadWriter interface {
	Read(io.Reader) error
	Write(io.Writer) error
}

type File struct {
	Callback ReadWriter

	path string
}

func NewFile(path string) *File {
	return &File{path: path}
}

func (f *File) Load() error {
	file, err := os.Open(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	if err := f.Callback.Read(file); err != nil {
		return err
	}
	return nil
}

func (f *File) getTmpPath() string {
	return f.path + ".tmp"
}

func (f *File) write() error {
	file, err := os.Create(f.getTmpPath())
	if err != nil {
		return err
	}
	defer file.Close()
	if err := f.Callback.Write(file); err != nil {
		return err
	}
	
	return nil
}

func (f *File) Save() error {
	if err := f.write(); err != nil {
		return err
	}
	if err := os.Rename(f.getTmpPath(), f.path); err != nil {
		return err
	}
	
	return nil
}
