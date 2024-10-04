package k8s

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
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

func getPodInfo(item *metaItem, isWhite bool) *corev1.Pod {
	podInfo := &corev1.Pod{}
	podInfo.Namespace = string(item.namespace)
	podInfo.Name = string(item.podName)
	podInfo.Status.ContainerStatuses = make([]corev1.ContainerStatus, 1)
	podInfo.Status.ContainerStatuses[0].Name = string(item.containerName)
	podInfo.Status.ContainerStatuses[0].ContainerID = "containerd://" + string(item.containerID)
	podInfo.Spec.NodeName = string(item.nodeName)
	if isWhite {
		podInfo.Labels = map[string]string{"allowed_label": "allowed_value"}
	} else {
		podInfo.Labels = map[string]string{"denied_label": "some_value"}
	}
	return podInfo
}

func config() *Config {
	config := &Config{AllowedPodLabels: []string{"allowed_label"}, OffsetsFile: "offsets.yaml"}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})
	return config
}

func TestEnrichment(t *testing.T) {
	nodeLabels = map[string]string{"zone": "z34"}
	p, input, _ := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	item := &metaItem{
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item, true)
	putMeta(podInfo)
	selfNodeName = "node_1"

	var (
		k8sPod           string
		k8sNamespace     string
		k8sContainer     string
		k8sNode          string
		k8sNodeLabelZone string
	)
	input.SetCommitFn(func(e *pipeline.Event) {
		k8sPod = strings.Clone(e.Root.Dig("k8s_pod").AsString())
		k8sNamespace = strings.Clone(e.Root.Dig("k8s_namespace").AsString())
		k8sContainer = strings.Clone(e.Root.Dig("k8s_container").AsString())
		k8sNode = strings.Clone(e.Root.Dig("k8s_node").AsString())
		k8sNodeLabelZone = strings.Clone(e.Root.Dig("k8s_node_label_zone").AsString())
		wg.Done()
	})

	filename := getLogFilename("/k8s-logs", item)
	input.In(0, filename, test.Offset(0), []byte(`{"time":"time","log":"log\n"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, "advanced-logs-checker-1566485760-trtrq", k8sPod, "wrong event field")
	assert.Equal(t, "sre", k8sNamespace, "wrong event field")
	assert.Equal(t, "duty-bot", k8sContainer, "wrong event field")
	assert.Equal(t, "node_1", k8sNode, "wrong event field")
	assert.Equal(t, "z34", k8sNodeLabelZone, "wrong event field")
}

func TestAllowedLabels(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(2)

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1111111111-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	putMeta(getPodInfo(item, true))
	filename1 := getLogFilename("/k8s-logs", item)

	item = &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-2222222222-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	putMeta(getPodInfo(item, false))
	filename2 := getLogFilename("/k8s-logs", item)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, filename1, test.Offset(0), []byte(`{"time":"time","log":"log\n"}`))
	input.In(0, filename2, test.Offset(0), []byte(`{"time":"time","log":"log\n"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, "allowed_value", outEvents[0].Root.Dig("k8s_pod_label_allowed_label").AsString(), "no label in event")
	assert.Nil(t, outEvents[1].Root.Dig("k8s_label_denied_label"), "extra label in event")
}

func TestK8SJoin(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(4)

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item, true)
	putMeta(podInfo)

	outLogs := make([]string, 0)
	outOffsets := make([]int64, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outLogs = append(outLogs, strings.Clone(e.Root.Dig("log").AsEscapedString()))
		outOffsets = append(outOffsets, e.Offset)
		wg.Done()
	})

	filename := getLogFilename("/k8s-logs", item)
	input.In(0, filename, test.Offset(10), []byte(`{"ts":"time","stream":"stdout","log":"one line log 1\n"}`))
	input.In(0, filename, test.Offset(20), []byte(`{"ts":"time","stream":"stderr","log":"error "}`))
	input.In(0, filename, test.Offset(30), []byte(`{"ts":"time","stream":"stdout","log":"this "}`))
	input.In(0, filename, test.Offset(40), []byte(`{"ts":"time","stream":"stdout","log":"is "}`))
	input.In(0, filename, test.Offset(50), []byte(`{"ts":"time","stream":"stdout","log":"joined "}`))
	input.In(0, filename, test.Offset(60), []byte(`{"ts":"time","stream":"stdout","log":"log 2\n"}`))
	input.In(0, filename, test.Offset(70), []byte(`{"ts":"time","stream":"stderr","log":"joined\n"}`))
	input.In(0, filename, test.Offset(80), []byte(`{"ts":"time","stream":"stdout","log":"one line log 3\n"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 4, len(outLogs))

	logs := []string{"\"one line log 1\\n\"", "\"error joined\\n\"", "\"this is joined log 2\\n\"", "\"one line log 3\\n\""}
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
		assert.True(t, index != -1, "log %s isn't found", log)

		assert.Equal(t, offsets[index], offset, "wrong offset")

		logs[index] = logs[len(logs)-1]
		offsets[index] = offsets[len(offsets)-1]

		logs = logs[:len(logs)-1]
		offsets = offsets[:len(offsets)-1]
	}

	check(outLogs[0], outOffsets[0])
	check(outLogs[1], outOffsets[1])
	check(outLogs[2], outOffsets[2])
	check(outLogs[3], outOffsets[3])
}

func TestCleanUp(t *testing.T) {
	p, _, _ := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))

	enableGatherer(logger.Instance)

	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-1",
		containerName: "duty-bot",
		containerID:   "1111111111111111111111111111111111111111111111111111111111111111",
	}, true))
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1566485760-2",
		containerName: "duty-bot",
		containerID:   "2222222222222222222222222222222222222222222222222222222222222222",
	}, true))
	putMeta(getPodInfo(&metaItem{
		nodeName:      "node_1",
		namespace:     "infra",
		podName:       "advanced-logs-checker-1566485760-3",
		containerName: "duty-bot",
		containerID:   "3333333333333333333333333333333333333333333333333333333333333333",
	}, true))

	time.Sleep(metaExpireDuration + MaintenanceInterval)

	disableGatherer()
	p.Stop()
	assert.Equal(t, 0, len(metaData))
}

func TestParseLogFilename(t *testing.T) {
	ns, pod, container, cid := parseLogFilename("/k8s-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log")

	assert.Equal(t, namespace("sre"), ns)
	assert.Equal(t, podName("advanced-logs-checker-1566485760-trtrq"), pod)
	assert.Equal(t, containerName("duty-bot"), container)
	assert.Equal(t, containerID("4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0"), cid)
}
