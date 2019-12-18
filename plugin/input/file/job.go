package file

import (
	"os"
	"sync"
)

type job struct {
	file     *os.File
	inode    inode
	filename string
	symlink  string

	isDone   bool
	skipLine bool

	offsets streamsOffsets

	mu *sync.Mutex
}
