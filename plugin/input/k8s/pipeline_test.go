package k8s

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	p := test.NewPipeline(nil, "passive")
	config := config()
	dir, err := os.MkdirTemp(os.TempDir(), "file.d_k8s")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config.FileConfig.WatchingDir = dir
	config.FileConfig.Meta = cfg.MetaTemplates{
		"filename": "{{ .filename }}",
	}
	setInput(p, config)

	wg := sync.WaitGroup{}
	wg.Add(1)
	var (
		k8sPod         string
		k8sNamespace   string
		k8sContainer   string
		k8sFilename    string
		k8sContainerID string
	)
	setOutput(p, func(e *pipeline.Event) {
		k8sPod = strings.Clone(e.Root.Dig("k8s_pod").AsString())
		k8sNamespace = strings.Clone(e.Root.Dig("k8s_namespace").AsString())
		k8sContainer = strings.Clone(e.Root.Dig("k8s_container").AsString())
		k8sContainerID = strings.Clone(e.Root.Dig("k8s_container_id").AsString())
		k8sFilename = strings.Clone(e.Root.Dig("filename").AsString())
		wg.Done()
	})

	item := &metaItem{
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	filename := getLogFilename(dir, item)

	p.Start()
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}

	file.WriteString(`{"time":"time","log":"log\n"}` + "\n")
	file.Close()
	wg.Wait()
	p.Stop()

	assert.Equal(t, string(item.podName), k8sPod, "wrong event field")
	assert.Equal(t, string(item.namespace), k8sNamespace, "wrong event field")
	assert.Equal(t, string(item.containerName), k8sContainer, "wrong event field")
	assert.Equal(t, string(filename), k8sFilename, "wrong event field")
	assert.Equal(t, string(item.containerID), k8sContainerID, "wrong event field")
}
