package file

import (
	"bytes"
	"io"

	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/zap"
)

type worker struct {
}

func (w *worker) start(inputController pipeline.InputPluginController, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	go w.work(inputController, jobProvider, readBufferSize, logger)
}

func (w *worker) work(controller pipeline.InputPluginController, jobProvider *jobProvider, readBufferSize int, logger *zap.SugaredLogger) {
	accumBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	var seqID uint64 = 0
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
		skipLine := job.shouldSkip
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

		isEOF := false
		wasPut := false

		readTotal := int64(0)
		accumulated := int64(0)
		processed := int64(0)

		accumBuffer = accumBuffer[:0]
		for {
			readBuffer = readBuffer[:readBufferSize]
			r, err := file.Read(readBuffer)
			read := int64(r)
			readBuffer = readBuffer[:read]

			if err == io.EOF || read == 0 {
				isEOF = true
				break
			}

			if err != nil {
				logger.Fatalf("file %d:%s read error, %s read=%d", sourceID, sourceName, read, err.Error())
			}

			processed = 0
			for {
				if processed >= int64(len(readBuffer)) {
					break
				}

				pos := int64(bytes.IndexByte(readBuffer[processed:], '\n'))
				if pos == -1 {
					break
				}
				pos += processed

				// skip first event because file may be opened while event isn't completely written
				if skipLine {
					job.shouldSkip = false
					skipLine = false
				} else {
					offset := lastOffset + accumulated + pos + 1
					if len(accumBuffer) != 0 {
						accumBuffer = append(accumBuffer, readBuffer[processed:pos]...)
						seqID = controller.In(sourceID, sourceName, offset, accumBuffer, isVirgin)
					} else {
						seqID = controller.In(sourceID, sourceName, offset, readBuffer[processed:pos], isVirgin)
					}
					job.lastEventSeq = seqID
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
			}
		}

		// don't consider accumulated buffer cause we haven't put any events
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

		// check if file was truncated
		if isEOF {
			stat, err := file.Stat()
			if err != nil {
				logger.Fatalf("file %d:%s stat error: %s", sourceID, sourceName, err.Error())
			}

			// file was truncated
			if lastOffset+readTotal > stat.Size() {
				jobProvider.truncateJob(job)
			}

			jobProvider.doneJob(job)
		} else {
			jobProvider.continueJob(job)
		}
	}
}
