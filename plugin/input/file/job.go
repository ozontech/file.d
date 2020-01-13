package file

import (
	"os"
	"sync"
)

type job struct {
	file        *os.File
	inode       inode
	fingerprint fingerprint
	filename    string
	symlink     string

	isDone     bool
	shouldSkip bool

	offsets streamsOffsets

	mu *sync.Mutex
}
