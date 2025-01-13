package file

import (
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	k8s_meta "github.com/ozontech/file.d/plugin/input/k8s/meta"

	"go.uber.org/zap"
)

type worker struct {
	maxEventSize       int
	cutOffEventByLimit bool

	metaTemplater *metadata.MetaTemplater
	needK8sMeta   bool
}

type inputer interface {
	// In processes event and returns it seq number.
	In(sourceID pipeline.SourceID, sourceName string, offset pipeline.Offsets, data []byte, isNewSource bool, meta metadata.MetaData) uint64
	IncReadOps()
	IncMaxEventSizeExceeded(lvs ...string)
}

func (w *worker) start(inputController inputer, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	for metaParam := range jobProvider.config.Meta {
		if strings.Contains(metaParam, "k8s") {
			w.needK8sMeta = true
			break
		}
	}
	go w.work(inputController, jobProvider, readBufferSize, logger)
}

func (w *worker) work(controller inputer, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	accumBuf := make([]byte, 0, readBufferSize)
	readBuf := make([]byte, readBufferSize)
	shouldCheckMax := w.maxEventSize != 0
	for {
		job := <-jobProvider.jobsChan
		if job == nil {
			return
		}
		job.mu.Lock()
		file := job.file
		isDone := job.isDone
		isVirgin := job.isVirgin
		sourceID := job.sourceID
		sourceName := job.filename
		skipLine := job.shouldSkip.Load()
		lastOffset := job.curOffset
		offsets := job.offsets
		if job.symlink != "" {
			sourceName = job.symlink
		}
		job.mu.Unlock()

		if isDone {
			logger.Panicf("job is done, why worker should work?")
		}

		isEOFReached := false
		readTotal := int64(0)
		scanned := int64(0)

		var metadataInfo metadata.MetaData
		if w.metaTemplater != nil {
			metaData, err := newMetaInformation(
				job.filename,
				job.symlink,
				job.inode,
				w.needK8sMeta,
			)
			if err != nil {
				logger.Error("cannot parse meta info", zap.Error(err))
			}
			metadataInfo, err = w.metaTemplater.Render(metaData)
			if err != nil {
				logger.Error("cannot render meta info", zap.Error(err))
			}
		}

		// append the data of the old work, this happens when the event was not completely written to the file
		// for example: {"level": "info", "message": "some...
		// the end of the message can be added later and will be read in this iteration
		accumBuf = append(accumBuf[:0], job.tail...)
		for {
			n, err := file.Read(readBuf)
			controller.IncReadOps()
			// if we read to end of file it's time to check truncation etc and process next job
			if err == io.EOF || n == 0 {
				isEOFReached = true
				break
			}
			if err != nil {
				logger.Fatalf("file %d:%s read error, %s read=%d", sourceID, sourceName, err.Error(), n)
			}

			read := int64(n)
			readTotal += read
			buf := readBuf[:read]

			// readBuf parsing loop
			// It can contain either one long event that did not fit in the buffer, or many different events
			for len(buf) != 0 {
				// \n is a line separator, -1 means that the file doesn't have new valid logs
				pos := int64(bytes.IndexByte(buf, '\n'))

				// if this is not the end of the event
				if pos == -1 {
					scanned += int64(len(buf))
					break
				}
				line := buf[:pos+1]
				buf = buf[pos+1:]

				scanned += pos + 1

				// check if the event fits into the max size, otherwise skip the event
				if shouldCheckMax && !w.cutOffEventByLimit && len(accumBuf)+len(line) > w.maxEventSize {
					controller.IncMaxEventSizeExceeded(sourceName)
					skipLine = true
				}

				// skip first event because file may be opened while event isn't completely written.
				if skipLine {
					job.shouldSkip.Store(false)
					skipLine = false
				} else {
					inBuf := line
					// if some data have been accumulated then append the line to it
					if len(accumBuf) != 0 {
						accumBuf = append(accumBuf, line...)
						inBuf = accumBuf
					}

					job.lastEventSeq = controller.In(sourceID, sourceName, pipeline.NewOffsets(lastOffset+scanned, offsets), inBuf, isVirgin, metadataInfo)
				}
				// restore the line buffer
				accumBuf = accumBuf[:0]
			}

			// check the buffer size and limits to avoid OOM if event is long
			if shouldCheckMax && len(accumBuf) > w.maxEventSize {
				if !w.cutOffEventByLimit {
					continue
				}
				accumBuf = accumBuf[:w.maxEventSize]
			}
			accumBuf = append(accumBuf, buf...)
		}

		// tail of read is in accumBuf, save it
		job.tail = append(job.tail[:0], accumBuf...)
		job.curOffset += readTotal

		// check if file was truncated.
		if isEOFReached {
			err := w.processEOF(file, job, jobProvider, lastOffset+readTotal)
			if err != nil {
				logger.Fatalf("file %d:%s stat error: %s", sourceID, sourceName, err.Error())
			}
		} else {
			// put job in the end of queue.
			jobProvider.continueJob(job)
		}
	}
}

func (w *worker) processEOF(file *os.File, job *Job, jobProvider *jobProvider, totalOffset int64) error {
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// files truncated from time to time, after logs from file was processed.
	// Position > stat.Size() means that data was truncated and
	// caret pointer must be moved to start of file.
	if totalOffset > stat.Size() {
		jobProvider.truncateJob(job)
	}

	// Mark job as done till new lines has appeared.
	jobProvider.doneJob(job)

	return nil
}

type metaInformation struct {
	filename string
	symlink  string
	inode    uint64

	k8sMetadata *k8s_meta.K8sMetaInformation
}

func newMetaInformation(filename, symlink string, inode inodeID, parseK8sMeta bool) (metaInformation, error) {
	var metaData k8s_meta.K8sMetaInformation
	var err error
	if parseK8sMeta {
		metaData, err = k8s_meta.NewK8sMetaInformation(symlink)
		if err != nil {
			metaData, err = k8s_meta.NewK8sMetaInformation(filename)
			if err != nil {
				return metaInformation{}, err
			}
		}
	}

	return metaInformation{
		filename:    filename,
		symlink:     symlink,
		inode:       uint64(inode),
		k8sMetadata: &metaData,
	}, nil
}

func (m metaInformation) GetData() map[string]any {
	data := map[string]any{
		"filename": m.filename,
		"symlink":  m.symlink,
		"inode":    m.inode,
	}

	if m.k8sMetadata != nil {
		data["pod_name"] = m.k8sMetadata.PodName
		data["namespace"] = m.k8sMetadata.Namespace
		data["container_name"] = m.k8sMetadata.ContainerName
		data["container_id"] = m.k8sMetadata.ContainerID
		data["pod"] = m.k8sMetadata.Pod
	} else {
		data["pod_name"] = nil
		data["namespace"] = nil
		data["container_name"] = nil
		data["container_id"] = nil
		data["pod"] = nil
	}

	return data
}

/*{ meta-params
**`filename`**

**`symlink`**

**`inode`**
}*/
