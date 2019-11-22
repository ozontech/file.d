package file

import (
	"io"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type worker struct {
}

func (w *worker) start(index int, inputController pipeline.InputPluginController, jobProvider *jobProvider, readBufferSize int) {
	go w.work(index, inputController, jobProvider, readBufferSize)
}

func (w *worker) work(index int, controller pipeline.InputPluginController, jobProvider *jobProvider, readBufferSize int) {
	accumBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	for {
		job := <-jobProvider.jobsChan
		if job == nil {
			return
		}

		job.mu.Lock()
		file := job.file
		isDone := job.isDone
		sourceID := pipeline.SourceID(job.inode)
		sourceName := job.filename
		skipLine := job.skipLine
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
			for i := int64(0); i < read; i++ {
				if readBuffer[i] != '\n' {
					continue
				}

				// skip first event because it may lost first part
				if skipLine {
					job.skipLine = false
				} else {
					offset := lastOffset + accumulated + i + 1
					if len(accumBuffer) != 0 {
						accumBuffer = append(accumBuffer, readBuffer[processed:i]...)
						controller.In(sourceID, sourceName, offset, accumBuffer)
					} else {
						controller.In(sourceID, sourceName, offset, readBuffer[processed:i])
					}
				}
				accumBuffer = accumBuffer[:0]

				processed = i + 1
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
