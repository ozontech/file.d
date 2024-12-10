package file_es

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

type Config struct {
	ctx    context.Context
	cancel func()

	inputDir string
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.ctx, c.cancel = context.WithTimeout(context.Background(), time.Minute*2)

	c.inputDir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.inputDir)
	input.Set("filename_pattern", "input.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))
}

const (
	n            = 10
	successEvent = `{"field_a":"AAAA","field_b":"BBBB"}`
	failEvent    = `{"field_a":"AAAA","field_b":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`
)

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer func() {
		_ = file.Close()
	}()

	for i := 0; i < n; i++ {
		err = addEvent(file, successEvent)
		require.NoError(t, err)
	}

	err = addEvent(file, failEvent)
	require.NoError(t, err)

	for i := 0; i < 2*n-1; i++ {
		err = addEvent(file, successEvent)
		require.NoError(t, err)
	}

	_ = file.Sync()
}

func addEvent(f *os.File, s string) error {
	_, err := f.WriteString(s + "\n")
	return err
}

func (c *Config) Validate(t *testing.T) {
	time.Sleep(5 * time.Second)

	count, err := c.getEventsCount()
	require.NoError(t, err)
	require.Equal(t, n, count)

	err = c.deleteAll()
	require.NoError(t, err)
}

func (c *Config) deleteAll() error {
	client := &http.Client{Timeout: 3 * time.Second}

	req, err := http.NewRequest(http.MethodDelete, "http://127.0.0.1:9200/seq-db", http.NoBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Add("Authorization", "elastic:password")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read all: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wrong status code; status = %d; body = %s", resp.StatusCode, respBody)
	}

	return nil
}

type searchResp struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		}
	} `json:"hits"`
}

func (c *Config) getEventsCount() (int, error) {
	client := &http.Client{Timeout: 3 * time.Second}

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:9200/_search", http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Add("Authorization", "elastic:password")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read all: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("wrong status code; status = %d; body = %s", resp.StatusCode, respBody)
	}

	var respData searchResp
	err = json.Unmarshal(respBody, &respData)
	if err != nil {
		return 0, err
	}

	return respData.Hits.Total.Value, nil
}
