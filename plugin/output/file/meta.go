package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/ozontech/file.d/offset"
)

const (
	metaFilePrefix      = "meta_"
	firstTimestampField = "first_timestamp"
	lastTimestampField  = "last_timestamp"
)

type meta struct {
	filePreifx,
	fileName,
	separator,
	dir,
	extention,
	sealedFileFieldName,
	sealedFilePathFieldName string
	staticMeta map[string]interface{}

	fileMode int64
	mu       sync.Mutex

	finalFilePrefix string
	tsFileName      string
	file            *os.File
}

type metaInit struct {
	fileName,
	separator,
	dir,
	extention,
	filePrefix,
	sealedPrefix,
	staticMeta,
	sealedFileFieldName,
	sealedFilePathFieldName string
	fileMode int64
}

type sealUpDTO struct {
	firstTimestamp,
	lastTimestamp int64
	sealingLogFile,
	sealingOuterPath string
}

func newMeta(init metaInit) (*meta, error) {
	staticMetaMap := make(map[string]interface{})
	metaString := init.staticMeta
	if metaString != "" {
		if err := json.Unmarshal([]byte(metaString), &staticMetaMap); err != nil {
			return nil, err
		}
	}

	return &meta{
		fileName:                init.fileName,
		separator:               init.separator,
		dir:                     init.dir,
		extention:               init.extention,
		filePreifx:              init.filePrefix,
		finalFilePrefix:         init.sealedPrefix,
		fileMode:                init.fileMode,
		sealedFileFieldName:     init.sealedFileFieldName,
		sealedFilePathFieldName: init.sealedFilePathFieldName,
		staticMeta:              staticMetaMap,
	}, nil
}

// sealUpCurrentMeta creates finalized meta for sealed file
func (m *meta) sealUpCurrentMeta(sealupDTO sealUpDTO) error {
	// ignore during startup
	if m.tsFileName == "" {
		return nil
	}
	sealedName := m.finalFilePrefix + sealupDTO.sealingLogFile

	if err := m.updateMetaFile(sealupDTO.firstTimestamp, sealupDTO.lastTimestamp, sealupDTO.sealingOuterPath); err != nil {
		return err
	}

	currentMetaFileName := filepath.Join(m.dir, m.tsFileName)
	if err := os.Rename(currentMetaFileName, filepath.Join(m.dir, sealedName)); err != nil {
		return fmt.Errorf("can't rename metafile: %s", err.Error())
	}

	return nil
}

// newMetaFile creates new or pulls up old file
func (m *meta) newMetaFile(
	filename string,
	sealUp sealUpDTO,
	timestamp int64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sealUpCurrentMeta(sealUp); err != nil {
		return err
	}

	m.tsFileName = fmt.Sprintf("%s%d%s%s", m.filePreifx, timestamp, m.separator, filename)
	pattern := filepath.Join(m.dir, m.filePreifx+"*"+m.fileName+"."+m.extention)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("can't glob: pattern=%s, err=%ss", pattern, err.Error())
	}

	// Reuse old file
	if len(matches) == 1 {
		m.tsFileName = path.Base(matches[0])
	} else {
		m.tsFileName += "." + m.extention
	}
	metaName := filepath.Join(m.dir, m.tsFileName)

	// creates meta dir if required
	_, err = os.Stat(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(m.dir, os.ModePerm); err != nil {
				return fmt.Errorf("can't create meta dir: %s", err.Error())
			}
		} else {
			return fmt.Errorf("can't check if meta dir exists: %s", err.Error())
		}
	}

	metaFile, err := os.OpenFile(metaName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open or create file: %s, error: %s", metaName, err.Error())
	}
	m.file = metaFile

	return nil
}

func (m *meta) updateMetaFile(firstTimestamp, lastTimestamp int64, sealUpOuterPath string) error {
	values := make(map[string]interface{})
	for k, v := range m.staticMeta {
		values[k] = v
	}
	values[firstTimestampField] = firstTimestamp
	values[lastTimestampField] = lastTimestamp
	values[m.sealedFileFieldName] = filepath.Base(sealUpOuterPath)
	values[m.sealedFilePathFieldName] = sealUpOuterPath

	return offset.SaveJson(filepath.Join(m.dir, m.tsFileName), values)
}

// updateMetaFileWithLock keeps meta in actual state
func (m *meta) updateMetaFileWithLock(firstTimestamp, lastTimestamp int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.updateMetaFile(firstTimestamp, lastTimestamp, "")
}
