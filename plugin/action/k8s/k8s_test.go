package k8s

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
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

func startPipeline() (*pipeline.Pipeline, *fake.Plugin, *Plugin, *devnull.Plugin) {
	p := pipeline.New("k8s_pipeline", 16, 1)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginDescription{Plugin: inputPlugin, Config: fake.Config{}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{}
	p.Tracks[0].AddActionPlugin(&pipeline.ActionPluginDescription{Plugin: plugin, Config: config})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginDescription{Plugin: outputPlugin, Config: config})

	p.Start()

	return p, inputPlugin, plugin, outputPlugin
}

func TestEnrichment(t *testing.T) {
	p, input, _, _ := startPipeline()
	defer p.Stop()

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item)
	putMeta(podInfo)

	var event *pipeline.Event = nil
	filename := getLogFilename("/docker-logs", item)
	input.SetAcceptFn(func(e *pipeline.Event) {
		event = e
	})

	input.In(0, filename, 0, 0, []byte(`{"time":"time","log":"log\n"}`))
	input.Wait()

	assert.NotNil(t, event.JSON.Get("k8s_pod"), "can't find field")
	assert.Equal(t, `"advanced-logs-checker-1566485760-trtrq"`, event.JSON.Get("k8s_pod").String())

	assert.NotNil(t, event.JSON.Get("k8s_ns"), "can't find field")
	assert.Equal(t, `"sre"`, event.JSON.Get("k8s_ns").String())

	assert.NotNil(t, event.JSON.Get("k8s_container"), "can't find field")
	assert.Equal(t, `"duty-bot"`, event.JSON.Get("k8s_container").String())

	assert.NotNil(t, event.JSON.Get("k8s_node"), "can't find field")
	assert.Equal(t, `"node_1"`, event.JSON.Get("k8s_node").String())
}

func TestJoin(t *testing.T) {
	p, input, _, output := startPipeline()
	defer p.Stop()

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item)
	putMeta(podInfo)

	events := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		event := *e
		events = append(events, &event)
	})

	filename := getLogFilename("/docker-logs", item)
	input.In(0, filename, 0, 10, []byte(`{"ts":"time","stream":"stdout","log":"one line log 1\n"}`))
	input.In(0, filename, 10, 10, []byte(`{"ts":"time","stream":"stderr","log":"error "}`))
	input.In(0, filename, 20, 10, []byte(`{"ts":"time","stream":"stdout","log":"this "}`))
	input.In(0, filename, 30, 10, []byte(`{"ts":"time","stream":"stdout","log":"is "}`))
	input.In(0, filename, 40, 10, []byte(`{"ts":"time","stream":"stdout","log":"joined "}`))
	input.In(0, filename, 50, 10, []byte(`{"ts":"time","stream":"stdout","log":"log 2\n"}`))
	input.In(0, filename, 60, 10, []byte(`{"ts":"time","stream":"stderr","log":"joined\n"}`))
	input.In(0, filename, 70, 10, []byte(`{"ts":"time","stream":"stdout","log":"one line log 3\n"}`))
	p.HandleEventFlowFinish(true)
	p.WaitUntilDone(true)

	assert.Equal(t, 4, len(events))

	assert.Equal(t, "one line log 1\n", string(events[0].JSON.GetStringBytes("log")), "wrong event")
	assert.Equal(t, int64(10), events[0].Offset, "wrong offset")

	assert.Equal(t, "error joined\n", string(events[1].JSON.GetStringBytes("log")), "wrong event")
	assert.Equal(t, int64(70), events[1].Offset, "wrong offset")

	assert.Equal(t, "this is joined log 2\n", string(events[2].JSON.GetStringBytes("log")), "wrong event")
	assert.Equal(t, int64(60), events[2].Offset, "wrong offset")

	assert.Equal(t, "one line log 3\n", string(events[3].JSON.GetStringBytes("log")), "wrong event")
	assert.Equal(t, int64(80), events[3].Offset, "wrong offset")
}

func TestCleanUp(t *testing.T) {
	p, _, _, _ := startPipeline()
	defer p.Stop()

	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-1",
		containerName: "duty-bot",
		containerID:   "1111111111111111111111111111111111111111111111111111111111111111",
	}))
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-2",
		containerName: "duty-bot",
		containerID:   "2222222222222222222222222222222222222222222222222222222222222222",
	}))
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "infra",
		podName:       "advanced-logs-checker-1566485760-3",
		containerName: "duty-bot",
		containerID:   "3333333333333333333333333333333333333333333333333333333333333333",
	}))

	time.Sleep(metaExpireDuration + maintenanceInterval)

	assert.Equal(t, 0, len(metaData))
}

func TestParseDockerFilename(t *testing.T) {
	ns, pod, container, cid := parseDockerFilename("/docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Equal(t, namespace("sre"), ns)
	assert.Equal(t, podName("advanced-logs-checker-1566485760-trtrq"), pod)
	assert.Equal(t, containerName("duty-bot"), container)
	assert.Equal(t, containerID("4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0"), cid)
}
