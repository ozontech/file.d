package input_file

import (
	"gitlab.ozon.ru/sre/filed/global"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"io"
)

type worker struct {
	id string
}

func (w *worker) start(parser *pipeline.Parser, workProvider *jobProvider, readBufferSize int) {
	global.Logger.Infof("worker %q started", w.id)

	concatBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	for {
		job := <-workProvider.nextJob

		file := job.file

		concatBuffer = concatBuffer[0:0]
		isEOF := false
		readTotal := 0
		processedTotal := 0
		processedConcat := 0
		wasPut := false
		for {
			read, err := file.Read(readBuffer)

			if err == io.EOF || read == 0 {
				isEOF = true
				break
			}

			if err != nil {
				global.Logger.Panicf("read error read=%d: %s", read, err.Error())
			}

			processed := int(0)
			for i := 0; i < read; i++ {
				if readBuffer[i] != '\n' {
					continue
				}

				if (len(concatBuffer) != 0) {
					concatBuffer = append(concatBuffer, readBuffer[processed:i]...)
					parser.Put(concatBuffer)
				} else {
					parser.Put(readBuffer[processed:i])
				}
				concatBuffer = concatBuffer[:0]

				processed = processedConcat + i + 1
				processedConcat = 0
			}

			processedTotal += processed
			readTotal += read

			wasPut = processed != 0
			if wasPut {
				break
			} else {
				concatBuffer = append(concatBuffer, readBuffer[0:read]...)
				processedConcat += read
			}
		}

		offsetBackward := processedTotal - readTotal
		if offsetBackward != 0 {
			_, err := file.Seek(int64(offsetBackward), io.SeekCurrent)
			if err != nil {
				global.Logger.Panicf("seek error: %s", err.Error())
			}
		}

		workProvider.releaseJob(job, isEOF)
	}
}
