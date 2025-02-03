package file_elasticsearch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func createIngestPipeline(elasticURL, pipelineID, username, password string) error {
	url := fmt.Sprintf("%s/_ingest/pipeline/%s", elasticURL, pipelineID)

	pipelineBody := `{"description":"test ingest pipeline","processors":[{"set":{"field":"processed_at","value":"{{_ingest.timestamp}}"}}]}`

	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(pipelineBody))
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
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read body response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status: %d, body: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func getDocumentsFromIndex(elasticURL, indexName, username, password string) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s/%s/_search", elasticURL, indexName)

	body := `{"query":{"match_all":{}}}`

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
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
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d, response: %s", resp.StatusCode, string(respBody))
	}

	type searchResponse struct {
		Hits struct {
			Hits []struct {
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	var result searchResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	resultDocs := make([]map[string]interface{}, 0, len(result.Hits.Hits))

	for _, hit := range result.Hits.Hits {
		resultDocs = append(resultDocs, hit.Source)
	}

	return resultDocs, nil
}

func waitUntilIndexReady(elasticURL, indexName, username, password string, minDocs, retries int, delay time.Duration) error {
	client := &http.Client{
		Timeout: time.Second,
	}

	url := fmt.Sprintf("%s/%s/_count", elasticURL, indexName)
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	for i := 0; i < retries; i++ {
		ok, err := func() (bool, error) {
			resp, err := client.Do(req)
			if err != nil {
				return false, fmt.Errorf("failed to make request: %w", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusServiceUnavailable {
				return false, nil
			}

			if resp.StatusCode != http.StatusOK {
				return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return false, fmt.Errorf("failed to read response: %w", err)
			}

			var result map[string]interface{}
			if err := json.Unmarshal(body, &result); err != nil {
				return false, fmt.Errorf("failed to decode response: %w", err)
			}

			if count, ok := result["count"].(float64); ok {
				if int(count) >= minDocs {
					return true, nil
				}
			} else {
				return false, fmt.Errorf("unexpected response structure")
			}

			return false, nil
		}()

		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(delay)
	}

	return fmt.Errorf("index '%s' did not meet conditions after %d retries", indexName, retries)
}
