//go:build e2e_new

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	http2 "net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/test"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
General interface for e2e tests
AddConfigSettings - function that preparing а config for your test
Send - function that send message in pipeline and waits for the end of processing
Validate - function that validate result of the work
*/

type e2eTest interface {
	AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string)
	Send(t *testing.T)
	Validate(t *testing.T)
}

func startForTest(t *testing.T, e e2eTest, configPath, pipeName string) *fd.FileD {
	conf := cfg.NewConfigFromFile(configPath)
	e.AddConfigSettings(t, conf, pipeName)
	filed := fd.New(conf, "off")
	filed.Start()
	return filed
}

func TestE2EStabilityWorkCase(t *testing.T) {
	testsList := []struct {
		e2eTest
		cfgPath  string
		pipeName string
	}{
		{
			e2eTest: &fileFile{
				outputNamePattern: "/file-d.log",
				filePattern:       "pod_ns_container-*",
				count:             10,
				lines:             500,
				retTime:           "1s",
			},
			cfgPath:  "./../testdata/config/e2e_cfg/file_file.yaml",
			pipeName: "test_file_file",
		},
		{
			e2eTest: &httpFile{
				outputNamePattern: "/file-d.log",
				count:             10,
				lines:             500,
				retTime:           "1s",
			},
			cfgPath:  "./../testdata/config/e2e_cfg/http_file.yaml",
			pipeName: "test_http_file",
		},
		{
			e2eTest: &kafkaFile{
				topic:             "quickstart5",
				broker:            "localhost:9092",
				outputNamePattern: "/file-d.log",
				lines:             500,
				retTime:           "1s",
				partition:         4,
			},
			cfgPath:  "/Users/dsmolonogov/GolandProjects/file.d/testdata/config/e2e_cfg/kafka_file.yaml",
			pipeName: "test_kafka_file",
		},
	}

	for _, test := range testsList {
		startForTest(t, test.e2eTest, test.cfgPath, test.pipeName)
		test.Send(t)
		test.Validate(t)
	}
}

// Struct for file-file plugin e2e test
type fileFile struct {
	filesDir          string
	filePattern       string
	outputNamePattern string
	count             int
	lines             int
	retTime           string
}

func (f *fileFile) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	f.filesDir = t.TempDir()
	offsetsDir := t.TempDir()
	input := conf.Pipelines[pipeName].Raw.Get("input")
	input.Set("watching_dir", f.filesDir)
	input.Set("filename_pattern", f.filePattern)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", f.filesDir+f.outputNamePattern)
	output.Set("retention_interval", f.retTime)
}

func (f *fileFile) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(f.count)
	for i := 0; i < f.count; i++ {
		go func() {
			u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(f.filesDir, "pod_ns_container-"+u1+u2+".log")
			file, err := os.Create(name)
			if err != nil {
				assert.NoError(t, err, "unexpected error")
			}

			file.WriteString(strings.Repeat("{\"first_field\":\"second_field\"}\n", f.lines))
			_ = file.Close()
			wg.Done()
		}()
	}
	wg.Wait()

	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (f *fileFile) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", f.filesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, f.count*f.lines, test.CountLines(t, logFilePattern))
}

// Struct for http-file plugin e2e test
type httpFile struct {
	filesDir          string
	outputNamePattern string
	count             int
	lines             int
	retTime           string
}

func (h *httpFile) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	h.filesDir = t.TempDir()
	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", h.filesDir+h.outputNamePattern)
	output.Set("retention_interval", h.retTime)
}

func (h *httpFile) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(h.count)
	for i := 0; i < h.count; i++ {
		go func() {
			cl := http2.DefaultClient
			for i := 0; i < h.lines; i++ {
				rd := bytes.NewReader([]byte(`{"first_field":"second_field"}`))
				req, err := http2.NewRequest(http2.MethodPost, "http://localhost:9200/", rd)
				assert.Nil(t, err, "bad format http request")
				_, err = cl.Do(req)
				if err != nil {
					fmt.Println(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (h *httpFile) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", h.filesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, h.count*h.lines, test.CountLines(t, logFilePattern))
}

// Struct for kafka-file plugin e2e test
type kafkaFile struct {
	topic             string
	broker            string
	filesDir          string
	outputNamePattern string
	lines             int
	retTime           string
	partition         int
}

func (k *kafkaFile) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	k.filesDir = t.TempDir()
	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", k.filesDir+k.outputNamePattern)
	output.Set("retention_interval", k.retTime)
}

func (k *kafkaFile) Send(t *testing.T) {
	time.Sleep(10 * time.Second)
	connections := make([]*kafka.Conn, k.partition)
	for i := 0; i < k.partition; i++ {
		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", k.topic, i)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		connections[i] = conn
	}

	for i := 0; i < k.lines; i++ {
		for _, conn := range connections {
			_, err := conn.WriteMessages(
				kafka.Message{Value: []byte(`{"key":"value"}`)},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err)
			}
		}
	}

	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}
	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (k *kafkaFile) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", k.filesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, k.lines*k.partition, test.CountLines(t, logFilePattern))
}
