package journalctl

import (
	"fmt"
	"io"

	"github.com/ozonru/file.d/common"
)

type offsetInfo struct {
	current int64
	offset  int64
	cursor  string
	file    *common.File
}

func newOffsetInfo(path string) *offsetInfo {
	res := &offsetInfo{
		file: common.NewFile(path),
	}
	res.file.Callback = res
	return res
}

func (o *offsetInfo) set(cursor string) {
	o.cursor = cursor
	o.offset++
}

func (o *offsetInfo) Read(r io.Reader) error {
	if _, err := fmt.Fscanf(r, "- %d: %q", &o.offset, &o.cursor); err != nil {
		return err
	}
	o.current = o.offset + 1
	return nil
}

func (o *offsetInfo) Write(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "- %d: %q", o.offset, o.cursor); err != nil {
		return err
	}
	return nil
}

func (o *offsetInfo) load() error {
	return o.file.Load()
}

func (o *offsetInfo) save() error {
	return o.file.Save()
}
