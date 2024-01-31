//go:build linux

package journalctl

import (
	"bufio"
	"io"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type journalReaderConfig struct {
	output   io.Writer
	cursor   string
	logger   *zap.Logger
	maxLines int
}

type journalReader struct {
	config *journalReaderConfig
	cmd    *exec.Cmd
	args   []string

	// reader metrics
	readerErrorsMetric prometheus.Counter
}

func (r *journalReader) readLines(rd io.Reader, config *journalReaderConfig) {
	reader := bufio.NewReaderSize(rd, 1024*1024*10) // max message size
	totalLines := 0

	// cursor will point to the last message, that we sent
	// so we'll skip it, because we already sent it
	if config.cursor != "" {
		_, _, err := reader.ReadLine()
		if err != nil {
			r.config.logger.Fatal(err.Error())
		}
	}

	for {
		bytes, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			r.readerErrorsMetric.Inc()
			r.config.logger.Error(err.Error())
			continue
		}
		_, err = config.output.Write(bytes)
		if err != nil {
			r.readerErrorsMetric.Inc()
			r.config.logger.Error(err.Error())
		}

		totalLines++
		if config.maxLines > 0 && totalLines >= config.maxLines {
			break
		}
	}
}

func newJournalReader(config *journalReaderConfig, readerErrorsCounter prometheus.Counter) *journalReader {
	res := &journalReader{
		config:             config,
		readerErrorsMetric: readerErrorsCounter,
	}
	res.args = []string{
		"-o", "json",
	}
	if config.cursor != "" {
		res.args = append(res.args, "-c", config.cursor)
	} else {
		res.args = append(res.args, "-n", "all")
	}
	return res
}

func (r *journalReader) start() error {
	r.config.logger.Info(`running journalctl`, zap.String("args", strings.Join(r.args, " ")))
	r.cmd = exec.Command("journalctl", r.args...)

	out, err := r.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := r.cmd.Start(); err != nil {
		return err
	}

	go r.readLines(out, r.config)

	return nil
}

func (r *journalReader) stop() error {
	return r.cmd.Process.Kill()
}
