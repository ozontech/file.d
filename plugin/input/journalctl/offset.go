package journalctl

import (
	"fmt"
	"io"
	"os"
)

type offsetInfo struct {
	current int64
	offset  int64
	cursor  string
	path    string
	file    *os.File
}

func newOffsetInfo(path string) *offsetInfo {
	return &offsetInfo{
		path: path,
	}
}

func (o *offsetInfo) set(cursor string) {
	o.cursor = cursor
	o.offset++
}

func (o *offsetInfo) openFile() error {
	var err error
	o.file, err = os.OpenFile(o.path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (o *offsetInfo) closeFile() error {
	return o.file.Close()
}

func (o *offsetInfo) clearFile() error {
	err := o.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = o.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

func (o *offsetInfo) load(r io.Reader) error {
	if _, err := fmt.Fscanf(r, "- %d: %q", &o.offset, &o.cursor); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		return err
	}
	o.current = o.offset + 1
	return nil
}

func (o *offsetInfo) save(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "- %d: %q", o.offset, o.cursor); err != nil {
		return err
	}
	return nil
}
