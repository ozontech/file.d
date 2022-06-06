package file

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/ozontech/file.d/offset"
)

const (
	metaFilePrefix       = "meta_"
	sealedMetafilePrefix = "sealed_"
	firstTimestampField  = "first_timestamp"
	lastTimestampField   = "last_timestamp"
)

type meta struct {
	filePreifx string
	fileName   string
	separator  string
	dir        string
	extention  string

	fileMode int64
	mu       sync.Mutex

	finalFilePrefix string
	finalFileDir    string

	tsFileName string
	file       *os.File
}

func newMeta(
	fileName,
	separator,
	dir,
	extention,
	finalFileDir,
	filePrefix,
	finalFilePrefix string,
	fileMode int64,

) *meta {
	return &meta{
		fileName:        fileName,
		separator:       fileNameSeparator,
		dir:             dir,
		extention:       extention,
		finalFileDir:    finalFileDir,
		filePreifx:      filePrefix,
		finalFilePrefix: finalFilePrefix,
		fileMode:        fileMode,
	}
}

//sealup renames metafile to final_metafile (which stores for logging reasons) and deletes it.
func (m *meta) sealUpCurrentMeta() error {
	// ignore during startup
	if m.tsFileName == "" {
		return nil
	}

	currentMetaFileName := filepath.Join(m.dir, m.tsFileName)
	if err := os.Rename(
		currentMetaFileName,
		filepath.Join(m.dir, m.finalFilePrefix+m.tsFileName),
	); err != nil {
		return fmt.Errorf("can't rename metafile: %s", err.Error())
	}

	return nil
}

// newMetaFile creates new or pulls up old file.
func (m *meta) newMetaFile(
	filename string,
	timestamp int64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sealUpCurrentMeta(); err != nil {
		return err
	}

	m.tsFileName = fmt.Sprintf("%s%d%s%s", m.filePreifx, timestamp, m.separator, filename)
	pattern := filepath.Join(m.dir, m.filePreifx+"*"+m.fileName+"."+m.extention)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("can't glob: pattern=%s, err=%ss", pattern, err.Error())
	}

	// existing og meta means crash of prev run before if was saved to final file. Pull up old file instead of new
	if len(matches) == 1 {
		m.tsFileName = path.Base(matches[0])
	} else {
		m.tsFileName += ".yaml"
	}
	metaName := filepath.Join(m.dir, m.tsFileName)

	metaFile, err := os.OpenFile(metaName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(m.fileMode))
	if err != nil {
		return fmt.Errorf("could not open or create file: %s, error: %s", metaName, err.Error())
	}
	m.file = metaFile

	return nil
}

func (m *meta) updateMetaFile(
	firstTimestamp,
	lastTimestamp float64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	values := make(map[string]interface{})
	values[firstTimestampField] = int64(firstTimestamp * float64(time.Second))
	values[lastTimestampField] = int64(lastTimestamp * float64(time.Second))

	return offset.SaveYAML(filepath.Join(m.dir, m.tsFileName), values)
}
