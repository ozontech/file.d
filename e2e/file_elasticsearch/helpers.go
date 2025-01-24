package file_elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func createIngestPipeline(elasticURL, pipelineID, username, password string) error {
	url := fmt.Sprintf("%s/_ingest/pipeline/%s", elasticURL, pipelineID)

	pipelineBody := map[string]interface{}{
		"description": "test ingest pipeline",
		"processors": []interface{}{
			map[string]interface{}{
				"set": map[string]interface{}{
					"field": "processed_at",
					"value": "{{_ingest.timestamp}}",
				},
			},
		},
	}

	body, err := json.Marshal(pipelineBody)
	if err != nil {
		return fmt.Errorf("failed to marshal body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make HTTP request: %w", err)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read body response: %w", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status: %d, body: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func getDocumentsFromIndex(elasticURL, indexName, username, password string) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s/%s/_search", elasticURL, indexName)

	body := []byte(`{"query":{"match_all":{}}}`)

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make HTTP request: %w", err)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d, response: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	var resultDocs []map[string]interface{}

	if hits, ok := result["hits"].(map[string]interface{}); ok {
		if docs, ok := hits["hits"].([]interface{}); ok {
			for _, doc := range docs {
				mappedDoc, ok := doc.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("unexpected document structure")
				}
				source, ok := mappedDoc["_source"].(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("_source field has unexpected structure")
				}
				resultDocs = append(resultDocs, source)
			}
		}
	} else {
		return nil, fmt.Errorf("unexpected response structure")
	}

	return resultDocs, nil
}

func waitUntilIndexReady(elasticURL, indexName, username, password string, minDocs, retries int, delay time.Duration) error {
	client := &http.Client{
		Timeout: time.Second,
	}

	for i := 0; i < retries; i++ {
		url := fmt.Sprintf("%s/%s/_count", elasticURL, indexName)
		req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		if username != "" && password != "" {
			req.SetBasicAuth(username, password)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to make request: %w", err)
		}

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusServiceUnavailable {
			_ = resp.Body.Close()
			time.Sleep(delay)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to reade response: %w", err)
		}
		_ = resp.Body.Close()

		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		if count, ok := result["count"].(float64); ok {
			if int(count) >= minDocs {
				return nil
			}
		} else {
			return fmt.Errorf("unexpected response structure")
		}

		time.Sleep(delay)
	}

	return fmt.Errorf("index '%s' did not meet conditions after %d retries", indexName, retries)
}
