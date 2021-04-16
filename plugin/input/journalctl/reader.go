package journalctl

import (
	"bufio"
	"io"
	"os/exec"
	"strings"

	"go.uber.org/zap"
)

type journalReaderConfig struct {
	output   io.Writer
	cursor   string
	logger   *zap.SugaredLogger
	maxLines int
}

type journalReader struct {
	config *journalReaderConfig
	cmd    *exec.Cmd
	args   []string
}

func readLines(r io.Reader, config *journalReaderConfig) {
	reader := bufio.NewReaderSize(r, 1024*1024*10) // max message size
	totalLines := 0

	// cursor will point to the last message, that we sent
	// so we'll skip it, because we already sent it
	if config.cursor != "" {
		reader.ReadLine()
	}

	for {
		bytes, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			config.logger.Error(err)
			continue
		}
		_, err = config.output.Write(bytes)
		if err != nil {
			config.logger.Error(err)
		}

		totalLines++
		if config.maxLines > 0 && totalLines >= config.maxLines {
			break
		}
	}
}

func newJournalReader(config *journalReaderConfig) *journalReader {
	res := &journalReader{config: config}
	res.args = []string{
		"-o", "json",
		"-f",
		"-a",
	}
	if config.cursor != "" {
		res.args = append(res.args, "-c", config.cursor)
	} else {
		res.args = append(res.args, "-n", "all")
	}
	return res
}

func (r *journalReader) start() error {
	r.config.logger.Infof("Running \"journalctl %s\"", strings.Join(r.args, " "))
	r.cmd = exec.Command("journalctl", r.args...)
	out, err := r.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := r.cmd.Start(); err != nil {
		return err
	}

	go readLines(out, r.config)

	return nil
}

func (r *journalReader) stop() error {
	if err := r.cmd.Process.Kill(); err != nil {
		return err
	}

	return nil
}
