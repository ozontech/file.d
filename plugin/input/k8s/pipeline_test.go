package k8s

import (
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"
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
	setInput(p, config)

	wg := sync.WaitGroup{}
	wg.Add(1)
	var (
		k8sPod         string
		k8sNamespace   string
		k8sContainer   string
		k8sContainerID string
	)
	setOutput(p, func(e *pipeline.Event) {
		k8sPod = strings.Clone(e.Root.Dig("k8s_pod").AsString())
		k8sNamespace = strings.Clone(e.Root.Dig("k8s_namespace").AsString())
		k8sContainer = strings.Clone(e.Root.Dig("k8s_container").AsString())
		k8sContainerID = strings.Clone(e.Root.Dig("k8s_container_id").AsString())
		wg.Done()
	})

	item := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1566485760-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	meta.PutMeta(getPodInfo(item, true))
	filename := getLogFilename(dir, item)

	p.Start()
	file, err := os.Create(filename)
	if err != nil {
		logger.Fatalf("Error creating file: %s", err.Error())
		return
	}

	file.WriteString(`{"time":"time","log":"log\n"}` + "\n")
	file.Close()
	wg.Wait()
	p.Stop()

	assert.Equal(t, string(item.PodName), k8sPod, "wrong event field")
	assert.Equal(t, string(item.Namespace), k8sNamespace, "wrong event field")
	assert.Equal(t, string(item.ContainerName), k8sContainer, "wrong event field")
	assert.Equal(t, string(item.ContainerID), k8sContainerID, "wrong event field")
}
