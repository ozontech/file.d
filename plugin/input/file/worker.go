package file

import (
	"bytes"
	"io"
	"os"

	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

type worker struct {
	maxEventSize int
}

type inputer interface {
	// In processes event and returns it seq number.
	In(sourceID pipeline.SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64
}

func (w *worker) start(inputController inputer, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	longpanic.Go(func() { w.work(inputController, jobProvider, readBufferSize, logger) })
}

func (w *worker) work(controller inputer, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	var eventBuf []byte
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
		if job.symlink != "" {
			sourceName = job.symlink
		}
		job.mu.Unlock()

		if isDone {
			logger.Panicf("job is done, why worker should work?")
		}

		isEOFReached := false
		readTotal := int64(0)
		processed := int64(0)

		accumBuf = append(accumBuf[:0], job.tail...)
		for {
			// on each iteration cap is readBufferSize, so cut all extra bytes.
			n, err := file.Read(readBuf)
			// if we read to end of file time to check truncation etc and process next job.
			if err == io.EOF || n == 0 {
				isEOFReached = true
				break
			}
			if err != nil {
				logger.Fatalf("file %d:%s read error, %s read=%d", sourceID, sourceName, err.Error(), n)
			}

			read := int64(n)
			job.curOffset += read
			buf := readBuf[:read]

			processed = 0
			accumulated := int64(0)
			for len(buf) != 0 {
				// \n is line separator, -1 means that file doesn't have new valid logs.
				pos := int64(bytes.IndexByte(buf, '\n'))
				if pos == -1 {
					break
				}
				line := buf[:pos+1]
				buf = buf[pos+1:]

				processed += pos + 1

				// skip first event because file may be opened while event isn't completely written.
				if skipLine {
					job.shouldSkip.Store(false)
					skipLine = false
				} else {
					// put into inBuffer read data from readBuffer and accumBuffer (of not empty).
					if len(accumBuf) != 0 {
						accumBuf = append(accumBuf, line...)
						eventBuf = accumBuf
					} else {
						eventBuf = line
					}
					if shouldCheckMax && len(eventBuf) > w.maxEventSize {
						break
					}
					job.lastEventSeq = controller.In(sourceID, sourceName, lastOffset+processed+accumulated, eventBuf, isVirgin)
				}
				accumBuf = accumBuf[:0]
			}

			readTotal += read

			accumBuf = append(accumBuf, buf...)
			accumulated += read
			if shouldCheckMax && len(accumBuf) > w.maxEventSize {
				break
			}

			// if any event have been sent to pipeline then get a new job
			if processed != 0 {
				break
			}
		}

		// tail of read is in accumBuf, save it
		job.tail = append(job.tail[:0], accumBuf...)

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
