package k8s

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/inputfake"
	"gitlab.ozon.ru/sre/filed/plugin/outputdevnull"
	corev1 "k8s.io/api/core/v1"
)

func TestMain(m *testing.M) {
	// we are going to do work fucking fast
	maintenanceInterval = time.Millisecond * 100
	metaExpireDuration = time.Millisecond * 500
	disableMetaUpdates = true

	code := m.Run()
	os.Exit(code)
}

func getLogFilename(prefix string, item *metaItem) string {
	return fmt.Sprintf(
		"%s/%s_%s_%s-%s.log",
		prefix,
		item.podName,
		item.namespace,
		item.containerName,
		item.containerID,
	)
}

func getPodInfo(item *metaItem) *corev1.Pod {
	podInfo := &corev1.Pod{}
	podInfo.Namespace = string(item.namespace)
	podInfo.Name = string(item.podName)
	podInfo.Status.ContainerStatuses = make([]corev1.ContainerStatus, 1)
	podInfo.Status.ContainerStatuses[0].Name = string(item.containerName)
	podInfo.Status.ContainerStatuses[0].ContainerID = "docker://" + string(item.containerID)
	podInfo.Spec.NodeName = string(item.nodeName)
	return podInfo
}

func startPipeline() (*pipeline.Pipeline, *fake.FakePlugin, *K8SPlugin) {
	p := pipeline.New("k8s_pipeline", 16, 1)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.FakePlugin)
	p.SetInputPlugin(&pipeline.InputPluginDescription{Plugin: inputPlugin, Config: fake.Config{}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*K8SPlugin)
	config := &Config{}
	p.Tracks[0].AddActionPlugin(&pipeline.ActionPluginDescription{Plugin: plugin, Config: config})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.DevNullPlugin)
	p.SetOutputPlugin(&pipeline.OutputPluginDescription{Plugin: outputPlugin, Config: config})

	p.Start()

	return p, inputPlugin, plugin
}

func TestEnrichment(t *testing.T) {
	p, input, _ := startPipeline()
	defer p.Stop()

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item)
	putMeta(podInfo, false)

	var event *pipeline.Event = nil
	filename := getLogFilename("/docker-logs", item)
	json := "{}"
	input.SetAcceptFn(func(e *pipeline.Event) {
		event = e
	})

	input.Accept(0, filename, 0, 0, []byte(json))
	input.Wait()

	assert.Equal(t, 1, checkerLogs["advanced-logs-checker-1566485760-trtrq"])

	assert.NotNil(t, event.Value.Get("k8s_pod"), "can't find field")
	assert.Equal(t, `"advanced-logs-checker-1566485760-trtrq"`, event.Value.Get("k8s_pod").String())

	assert.NotNil(t, event.Value.Get("k8s_ns"), "can't find field")
	assert.Equal(t, `"sre"`, event.Value.Get("k8s_ns").String())

	assert.NotNil(t, event.Value.Get("k8s_container"), "can't find field")
	assert.Equal(t, `"duty-bot"`, event.Value.Get("k8s_container").String())

	assert.NotNil(t, event.Value.Get("k8s_node"), "can't find field")
	assert.Equal(t, `"node_1"`, event.Value.Get("k8s_node").String())
}

func TestCleanUp(t *testing.T) {
	p, _, _ := startPipeline()
	defer p.Stop()

	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-1",
		containerName: "duty-bot",
		containerID:   "1111111111111111111111111111111111111111111111111111111111111111",
	}), false)
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-2",
		containerName: "duty-bot",
		containerID:   "2222222222222222222222222222222222222222222222222222222222222222",
	}), false)
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "infra",
		podName:       "advanced-logs-checker-1566485760-3",
		containerName: "duty-bot",
		containerID:   "3333333333333333333333333333333333333333333333333333333333333333",
	}), false)

	time.Sleep(metaExpireDuration + maintenanceInterval)

	assert.Equal(t, 0, len(meta))
}

func TestParseDockerFilename(t *testing.T) {
	ns, pod, container, cid := parseDockerFilename("/docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Equal(t, namespace("sre"), ns)
	assert.Equal(t, podName("advanced-logs-checker-1566485760-trtrq"), pod)
	assert.Equal(t, containerName("duty-bot"), container)
	assert.Equal(t, containerID("4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0"), cid)
}
