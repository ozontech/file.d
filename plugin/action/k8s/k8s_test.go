package k8s

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	corev1 "k8s.io/api/core/v1"
)

func TestMain(m *testing.M) {
	// we are going to do work fucking fast
	MaintenanceInterval = time.Millisecond * 100
	metaExpireDuration = time.Millisecond * 500
	DisableMetaUpdates = true

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
	p := pipeline.New("k8s_pipeline", 2048, 1, prometheus.NewRegistry())

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: fake.Config{}}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

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

	assert.Equal(t, "advanced-logs-checker-1566485760-trtrq", event.Root.Dig("k8s_pod").AsString(), "wrong event field")
	assert.Equal(t, "sre", event.Root.Dig("k8s_namespace").AsString(), "wrong event field")
	assert.Equal(t, "duty-bot", event.Root.Dig("k8s_container").AsString(), "wrong event field")
	assert.Equal(t, "node_1", event.Root.Dig("k8s_node").AsString(), "wrong event field")
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
	input.In(0, filename, 10, 10, []byte(`{"ts":"time","stream":"stdout","log":"one line log 1\n"}`))
	input.In(0, filename, 20, 10, []byte(`{"ts":"time","stream":"stderr","log":"error "}`))
	input.In(0, filename, 30, 10, []byte(`{"ts":"time","stream":"stdout","log":"this "}`))
	input.In(0, filename, 40, 10, []byte(`{"ts":"time","stream":"stdout","log":"is "}`))
	input.In(0, filename, 50, 10, []byte(`{"ts":"time","stream":"stdout","log":"joined "}`))
	input.In(0, filename, 60, 10, []byte(`{"ts":"time","stream":"stdout","log":"log 2\n"}`))
	input.In(0, filename, 70, 10, []byte(`{"ts":"time","stream":"stderr","log":"joined\n"}`))
	input.In(0, filename, 80, 10, []byte(`{"ts":"time","stream":"stdout","log":"one line log 3\n"}`))

	// unlock input
	input.Commit(nil)
	input.Commit(nil)
	input.Commit(nil)
	input.Commit(nil)

	p.HandleEventFlowFinish(true)
	p.WaitUntilDone(true)
	input.Wait()

	assert.Equal(t, 4, len(events))

	logs := []string{"one line log 1\n", "error joined\n", "this is joined log 2\n", "one line log 3\n"}
	offsets := []int64{10, 70, 60, 80}

	check := func(log string, offset int64) {
		index := -1
		for i, c := range logs {
			if c == log {
				index = i
				break
			}

		}
		assert.Equal(t, len(logs), len(offsets), "lengths isn't equal")
		assert.True(t, index != -1, "log not found")

		assert.Equal(t, offsets[index], offset, "wrong offset")

		logs[index] = logs[len(logs)-1]
		offsets[index] = offsets[len(offsets)-1]

		logs = logs[:len(logs)-1]
		offsets = offsets[:len(offsets)-1]

	}

	check(events[0].Root.Dig("log").AsString(), events[0].Offset)
	check(events[1].Root.Dig("log").AsString(), events[1].Offset)
	check(events[2].Root.Dig("log").AsString(), events[2].Offset)
	check(events[3].Root.Dig("log").AsString(), events[3].Offset)
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

	time.Sleep(metaExpireDuration + MaintenanceInterval)

	assert.Equal(t, 0, len(metaData))
}

func TestParseDockerFilename(t *testing.T) {
	ns, pod, container, cid := parseDockerFilename("/docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Equal(t, namespace("sre"), ns)
	assert.Equal(t, podName("advanced-logs-checker-1566485760-trtrq"), pod)
	assert.Equal(t, containerName("duty-bot"), container)
	assert.Equal(t, containerID("4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0"), cid)
}
