package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/offset"
)

const (
	metaFilePrefix      = "meta_"
	firstTimestampField = "first_timestamp"
	lastTimestampField  = "last_timestamp"
)

type meta struct {
	filePrefix,
	fileName,
	separator,
	dir,
	sealedFileFieldName,
	sealedFilePathFieldName string
	staticMeta map[string]interface{}

	fileMode int64
	mu       sync.Mutex

	sealedFilePrefix string
	tsFileName       string
	file             *os.File
}

type metaInit struct {
	config *Config
	fileName,
	separator,
	filePrefix string
}

type sealUpDTO struct {
	firstTimestamp,
	lastTimestamp int64
	sealingLogFile,
	sealingOuterPath string
}

func newMeta(init metaInit) (*meta, error) {
	staticMetaMap := make(map[string]interface{})
	metaString := init.config.MetaCfg.StaticMeta
	if metaString != "" {
		if err := json.Unmarshal([]byte(metaString), &staticMetaMap); err != nil {
			return nil, err
		}
	}

	return &meta{
		fileName:                init.fileName,
		separator:               init.separator,
		dir:                     init.config.MetaCfg.MetaDataDir,
		filePrefix:              init.filePrefix,
		sealedFilePrefix:        init.config.MetaCfg.SealedMetaPrefix,
		sealedFileFieldName:     init.config.MetaCfg.SealedFileNameField,
		sealedFilePathFieldName: init.config.MetaCfg.SealedFilePathFieldName,
		staticMeta:              staticMetaMap,
		fileMode:                init.config.FileMode_,
	}, nil
}

// sealUpCurrentMeta creates finalized meta for sealed file.
func (m *meta) sealUpCurrentMeta(sealupDTO sealUpDTO) error {
	// ignore during startup
	if m.tsFileName == "" {
		return nil
	}
	sealedName := m.sealedFilePrefix + sealupDTO.sealingLogFile

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
	intPair *pair,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sealUpCurrentMeta(sealUp); err != nil {
		return err
	}

	m.tsFileName = fmt.Sprintf("%s%d%s%s", m.filePrefix, timestamp, m.separator, filename)
	pattern := filepath.Join(m.dir, m.filePrefix+"*"+m.fileName+".json")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("can't glob: pattern=%s, err=%ss", pattern, err.Error())
	}
	// Reuse old file
	if len(matches) == 1 {
		m.tsFileName = path.Base(matches[0])

		var result map[string]interface{}
		err := offset.LoadJson(filepath.Join(m.dir, m.tsFileName), &result)
		if err != nil {
			logger.Errorf("can't load json meta: %s", err.Error())
		}

		// update pair after fail
		if f, ok := result[firstTimestampField]; ok {
			if err := intPair.UpdatePairJsonNumber(f); err != nil {
				logger.Errorf("candidate isn't json.Number: %s", err.Error())
			}
		}
		if f, ok := result[lastTimestampField]; ok {
			if err := intPair.UpdatePairJsonNumber(f); err != nil {
				logger.Errorf("candidate isn't json.Number: %s", err.Error())
			}
		}
	} else {
		m.tsFileName += ".json"
	}
	metaName := filepath.Join(m.dir, m.tsFileName)

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
