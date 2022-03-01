package journalctl

import (
	"bufio"
	"io"
	"os/exec"
	"strings"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/stats"

	"go.uber.org/zap"
)

//nolint:unused
const (
	subsystemName = "input_journalctl"
	readerErrors  = "reader_errors"
)

type journalReaderConfig struct {
	output   io.Writer
	cursor   string
	logger   *zap.SugaredLogger
	maxLines int
}

//nolint:unused
type journalReader struct {
	config *journalReaderConfig
	cmd    *exec.Cmd
	args   []string
}

//nolint:unused
func readLines(r io.Reader, config *journalReaderConfig) {
	reader := bufio.NewReaderSize(r, 1024*1024*10) // max message size
	totalLines := 0

	// cursor will point to the last message, that we sent
	// so we'll skip it, because we already sent it
	if config.cursor != "" {
		_, _, err := reader.ReadLine()
		if err != nil {
			logger.Fatalf(err.Error())
		}
	}

	for {
		bytes, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			stats.GetCounter(subsystemName, readerErrors).Inc()
			config.logger.Error(err)
			continue
		}
		_, err = config.output.Write(bytes)
		if err != nil {
			stats.GetCounter(subsystemName, readerErrors).Inc()
			config.logger.Error(err)
		}

		totalLines++
		if config.maxLines > 0 && totalLines >= config.maxLines {
			break
		}
	}
}

//nolint:deadcode,unused
func newJournalReader(config *journalReaderConfig) *journalReader {
	res := &journalReader{config: config}
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

//nolint:unused
func (r *journalReader) start() error {
	r.config.logger.Infof(`running "journalctl %s"`, strings.Join(r.args, " "))
	r.cmd = exec.Command("journalctl", r.args...)

	out, err := r.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := r.cmd.Start(); err != nil {
		return err
	}

	longpanic.Go(func() { readLines(out, r.config) })

	return nil
}

//nolint:unused
func (r *journalReader) stop() error {
	return r.cmd.Process.Kill()
}
