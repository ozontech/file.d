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
	var inBuffer []byte
	accumBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
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
		if job.symlink != "" {
			sourceName = job.symlink
		}
		job.mu.Unlock()

		if isDone {
			logger.Panicf("job is done, why worker should work?")
		}

		lastOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatalf("can't get offset, file %d:%s seek error: %s", sourceID, sourceName, err.Error())
		}

		// have end of file reached.
		isEOF := false

		// have any event put.
		wasPut := false

		readTotal := int64(0)
		accumulated := int64(0)
		processed := int64(0)

		accumBuffer = accumBuffer[:0]
		for {
			// on each iteration cap is readBufferSize, so cut all extra bytes.
			readBuffer = readBuffer[:readBufferSize]
			r, err := file.Read(readBuffer)
			read := int64(r)
			readBuffer = readBuffer[:read]
			// if we read to end of file time to check truncation etc and process next job.
			if err == io.EOF || read == 0 {
				isEOF = true
				break
			}
			if err != nil {
				logger.Fatalf("file %d:%s read error, %s read=%d", sourceID, sourceName, err.Error(), read)
			}

			processed = 0
			for {
				if processed >= int64(len(readBuffer)) {
					break
				}

				// \n is line separator, -1 means that file doesn't have new valid logs.
				pos := int64(bytes.IndexByte(readBuffer[processed:], '\n'))
				if pos == -1 {
					break
				}

				// to keep our total progress over file reading.
				pos += processed

				// skip first event because file may be opened while event isn't completely written.
				if skipLine {
					job.shouldSkip.Store(false)
					skipLine = false
				} else {
					// get current offset for reading.
					offset := lastOffset + accumulated + pos + 1
					// put into inBuffer read data from readBuffer and accumBuffer (of not empty).
					if len(accumBuffer) != 0 {
						accumBuffer = append(accumBuffer, readBuffer[processed:pos+1]...)
						inBuffer = accumBuffer
					} else {
						inBuffer = readBuffer[processed : pos+1]
					}
					if shouldCheckMax && len(inBuffer) > w.maxEventSize {
						break
					}
					job.lastEventSeq = controller.In(sourceID, sourceName, offset, inBuffer, isVirgin)
				}
				accumBuffer = accumBuffer[:0]

				processed = pos + 1
			}

			readTotal += read

			wasPut = processed != 0
			if wasPut {
				break
			} else {
				accumBuffer = append(accumBuffer, readBuffer[:read]...)
				accumulated += read
				if shouldCheckMax && len(accumBuffer) > w.maxEventSize {
					break
				}
			}
		}

		// don't consider accumulated buffer because we haven't put any events.
		if !wasPut {
			accumulated = 0
		}

		backwardOffset := accumulated + processed - readTotal
		if backwardOffset != 0 {
			_, err := file.Seek(backwardOffset, io.SeekCurrent)
			if err != nil {
				logger.Fatalf("can't set offset, file %d:%s seek error: %s", sourceID, sourceName, err.Error())
			}
		}
		// check if file was truncated.
		if isEOF {
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
