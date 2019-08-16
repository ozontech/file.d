package input_file

import (
	"io"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type worker struct {
	shouldExit bool
}

func (w *worker) start(parser *pipeline.Parser, jobProvider *jobProvider, readBufferSize int) {
	go w.process(parser, jobProvider, readBufferSize)
}

func (w *worker) stop() {
	w.shouldExit = true
}

func (w *worker) process(parser *pipeline.Parser, jobProvider *jobProvider, readBufferSize int) {
	accumBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	for {
		job := <-jobProvider.nextJob
		if w.shouldExit {
			return
		}

		file := job.file
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			logger.Panicf("file % s seek error: %s", file.Name(), err.Error())
		}

		isEOF := false
		wasPut := false

		readTotal := int64(0)
		accumulated := int64(0)
		processed := int64(0)

		accumBuffer = accumBuffer[:0]
		for {
			r, err := file.Read(readBuffer)
			read := int64(r)

			if err == io.EOF || read == 0 {
				isEOF = true
				break
			}

			if err != nil {
				logger.Panicf("file %s read error, %s read=%d", file.Name(), read, err.Error())
			}

			processed = 0
			for i := int64(0); i < read; i++ {
				if readBuffer[i] != '\n' {
					continue
				}

				if len(accumBuffer) != 0 {
					accumBuffer = append(accumBuffer, readBuffer[processed:i]...)
					parser.Put(jobProvider, job.stream, offset+accumulated+i+1, accumBuffer)
				} else {
					parser.Put(jobProvider, job.stream, offset+i+1, readBuffer[processed:i])
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
			_, err := file.Seek(int64(backwardOffset), io.SeekCurrent)
			if err != nil {
				logger.Panicf("file % s seek error: %s", file.Name(), err.Error())
			}
		}

		jobProvider.releaseJob(job, isEOF)
	}
}
