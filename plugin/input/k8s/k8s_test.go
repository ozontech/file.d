package k8s

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func setInput(p *pipeline.Pipeline, config *Config) {
	plugin, _ := Factory()

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: plugin,
		},
	})
}

func setOutput(p *pipeline.Pipeline, out func(event *pipeline.Event)) {
	plugin, config := devnull.Factory()
	outputPlugin := plugin.(*devnull.Plugin)

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})

	outputPlugin.SetOutFn(out)
}

func TestMain(m *testing.M) {
	// we are going to do work fucking fast
	meta.MaintenanceInterval = time.Millisecond * 100
	meta.MetaExpireDuration = time.Millisecond * 500
	meta.DisableMetaUpdates = true

	code := m.Run()
	os.Exit(code)
}

func getLogFilename(prefix string, item *meta.MetaItem) string {
	return fmt.Sprintf(
		"%s/%s_%s_%s-%s.log",
		prefix,
		item.PodName,
		item.Namespace,
		item.ContainerName,
		item.ContainerID,
	)
}

func getPodInfo(item *meta.MetaItem, isWhite bool) *corev1.Pod {
	podInfo := &corev1.Pod{}
	podInfo.Namespace = string(item.Namespace)
	podInfo.Name = string(item.PodName)
	podInfo.Status.ContainerStatuses = make([]corev1.ContainerStatus, 1)
	podInfo.Status.ContainerStatuses[0].Name = string(item.ContainerName)
	podInfo.Status.ContainerStatuses[0].ContainerID = "containerd://" + string(item.ContainerID)
	if isWhite {
		podInfo.Labels = map[string]string{"allowed_label": "allowed_value"}
	} else {
		podInfo.Labels = map[string]string{"denied_label": "some_value"}
	}
	return podInfo
}

func config() *Config {
	config := &Config{
		AllowedPodLabels: []string{"allowed_label"},
		OffsetsFile:      "offsets.yaml",
		K8sMeta:          getTestMeta(),
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})
	return config
}

func getTestMeta() cfg.MetaTemplates {
	metaConfig := cfg.MetaTemplates{}
	setBuiltInMeta(metaConfig)
	return metaConfig
}

func TestAllowedLabels(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))

	wg := &sync.WaitGroup{}
	wg.Add(2)

	item := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1111111111-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	meta.PutMeta(getPodInfo(item, true))
	filename1 := getLogFilename("/k8s-logs", item)

	item2 := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-2222222222-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	meta.PutMeta(getPodInfo(item2, false))
	filename2 := getLogFilename("/k8s-logs", item2)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, filename1, test.NewOffset(0), []byte(wrapK8sInfo(`log\n`, item, "node1")))
	input.In(0, filename2, test.NewOffset(0), []byte(wrapK8sInfo(`log\n`, item2, "node1")))

	wg.Wait()
	p.Stop()

	assert.Equal(t, "allowed_value", outEvents[0].Root.Dig("k8s_pod_label_allowed_label").AsString(), "no label in event")
	assert.Nil(t, outEvents[1].Root.Dig("k8s_label_denied_label"), "extra label in event")
}

func TestK8SJoin(t *testing.T) {
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(MultilineActionFactory, config(), pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(4)

	item := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1566485760-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	podInfo := getPodInfo(item, true)
	meta.PutMeta(podInfo)

	outLogs := make([]string, 0)
	outOffsets := make([]int64, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outLogs = append(outLogs, strings.Clone(e.Root.Dig("log").AsEscapedString()))
		outOffsets = append(outOffsets, e.Offset)
		wg.Done()
	})

	k8sMeta := fmt.Sprintf(
		`,"k8s_pod":"%s","k8s_namespace":"%s","k8s_container_id":"%s","k8s_container":"%s"`,
		item.PodName, item.Namespace, item.ContainerID, item.ContainerName,
	)

	filename := getLogFilename("/k8s-logs", item)
	input.In(0, filename, test.NewOffset(10), []byte(`{"ts":"time","stream":"stdout","log":"one line log 1\n"`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(20), []byte(`{"ts":"time","stream":"stderr","log":"error "`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(30), []byte(`{"ts":"time","stream":"stdout","log":"this "`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(40), []byte(`{"ts":"time","stream":"stdout","log":"is "`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(50), []byte(`{"ts":"time","stream":"stdout","log":"joined "`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(60), []byte(`{"ts":"time","stream":"stdout","log":"log 2\n"`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(70), []byte(`{"ts":"time","stream":"stderr","log":"joined\n"`+k8sMeta+`}`))
	input.In(0, filename, test.NewOffset(80), []byte(`{"ts":"time","stream":"stdout","log":"one line log 3\n"`+k8sMeta+`}`))

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

	meta.EnableGatherer(logger.Instance)

	meta.PutMeta(getPodInfo(&meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1566485760-1",
		ContainerName: "duty-bot",
		ContainerID:   "1111111111111111111111111111111111111111111111111111111111111111",
	}, true))
	meta.PutMeta(getPodInfo(&meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1566485760-2",
		ContainerName: "duty-bot",
		ContainerID:   "2222222222222222222222222222222222222222222222222222222222222222",
	}, true))
	meta.PutMeta(getPodInfo(&meta.MetaItem{
		Namespace:     "infra",
		PodName:       "advanced-logs-checker-1566485760-3",
		ContainerName: "duty-bot",
		ContainerID:   "3333333333333333333333333333333333333333333333333333333333333333",
	}, true))

	time.Sleep(meta.MetaExpireDuration + meta.MaintenanceInterval)

	meta.DisableGatherer()
	p.Stop()
	assert.Equal(t, 0, len(meta.MetaData))
}
