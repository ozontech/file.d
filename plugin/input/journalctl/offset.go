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

func (o *offsetInfo) loadFile() error {
	file, err := os.Open(o.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}
	defer file.Close()

	return o.load(file)
}

func (o *offsetInfo) load(r io.Reader) error {
	if _, err := fmt.Fscanf(r, "- %d: %q", &o.offset, &o.cursor); err != nil {
		return err
	}
	o.current = o.offset + 1
	return nil
}

func (o *offsetInfo) saveFile() error {
	file, err := os.Create(o.path)
	if err != nil {
		return err
	}
	defer file.Close()

	return o.save(file)
}

func (o *offsetInfo) save(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "- %d: %q", o.offset, o.cursor); err != nil {
		return err
	}
	return nil
}
