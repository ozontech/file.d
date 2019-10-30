package file

import (
	"io"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type worker struct {
}

func (w *worker) start(inputController pipeline.Head, jobProvider *jobProvider, readBufferSize int) {
	go w.work(inputController, jobProvider, readBufferSize)
}

func (w *worker) work(head pipeline.Head, jobProvider *jobProvider, readBufferSize int) {
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
		sourceId := pipeline.SourceID(job.inode)
		sourceName := job.filename
		skipLine := job.skipLine
		if job.symlink != "" {
			sourceName = job.symlink
		}
		job.mu.Unlock()

		if isDone {
			logger.Panicf("job is done, why worker should work?")
		}

		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Fatalf("can't get offset, file %d:%s seek error: %s", sourceId, sourceName, err.Error())
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
				logger.Fatalf("file %d:%s read error, %s read=%d", sourceId, sourceName, read, err.Error())
			}

			processed = 0
			for i := int64(0); i < read; i++ {
				if readBuffer[i] != '\n' {
					continue
				}

				// skip first event because it may lost first part
				if skipLine {
					job.mu.Lock()
					job.skipLine = false
					job.mu.Unlock()
				} else {
					if len(accumBuffer) != 0 {
						accumBuffer = append(accumBuffer, readBuffer[processed:i]...)
						head.In(sourceId, sourceName, offset+accumulated+i+1, accumBuffer)
					} else {
						head.In(sourceId, sourceName, offset+i+1, readBuffer[processed:i])
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
				logger.Fatalf("can't set offset, file %d:%s seek error: %s", sourceId, sourceName, err.Error())
			}
		}

		// check if file was truncated
		if isEOF {
			stat, err := file.Stat()
			if err != nil {
				logger.Fatalf("file %d:%s stat error: %s", sourceId, sourceName, err.Error())
			}

			// file was truncated, seek to start
			if offset+readTotal > stat.Size() {
				jobProvider.truncateJob(job)
				isEOF = false
				offset = 0
				accumulated = 0
				processed = 0
			}
		}

		jobProvider.releaseJob(job, isEOF)
	}
}
