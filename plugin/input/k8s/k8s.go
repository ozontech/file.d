package k8s

import (
	"net/http"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/file"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

/*{ introduction
It reads Kubernetes logs and also adds pod meta-information. Also, it joins split logs into a single event.

We recommend using the [Helm-chart](/charts/filed/README.md) for running in Kubernetes

Source log file should be named in the following format:<br> `[pod-name]_[namespace]_[container-name]-[container-id].log`

E.g. `my_pod-1566485760-trtrq_my-namespace_my-container-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

An information which plugin adds:
* `k8s_node` – node name where pod is running;
* `k8s_node_label_*` – node labels;
* `k8s_pod` – pod name;
* `k8s_namespace` – pod namespace name;
* `k8s_container` – pod container name;
* `k8s_label_*` – pod labels.

> ⚠ Use add_file_name plugin if you want to add filename to events.

**Example:**
```yaml
pipelines:
  example_k8s_pipeline:
    input:
      type: k8s
      offsets_file: /data/offsets.yaml
      file_config:                        // customize file plugin
        persistence_mode: sync
        read_buffer_size: 2048
```

To allow the plugin to access the necessary Kubernetes resources, you need to create a ClusterRole that grants permissions to read pod and node information.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: filed-pod-watcher
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get"]
```
}*/

type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
	params *pipeline.InputPluginParams

	fp *file.Plugin
}

type Config struct {
	// ! config-params
	// ^ config-params

	// > @3@4@5@6
	// >
	// > Docker splits long logs by 16kb chunks. The plugin joins them back, but if an event is longer than this value in bytes, it will be split after all.
	// > > Due to the optimization process it's not a strict rule. Events may be split even if they won't exceed the limit.
	SplitEventSize int `json:"split_event_size" default:"1000000"` // *

	// > @3@4@5@6
	// >
	// > How many entries for deleted pods should be stored in the cache
	DeletedPodsCacheSize int `json:"deleted_pods_cache_size" default:"10000"` // *

	// > @3@4@5@6
	// >
	// > If set, it defines which pod labels to add to the event, others will be ignored.
	AllowedPodLabels  []string `json:"allowed_pod_labels" slice:"true"` // *
	AllowedPodLabels_ map[string]bool

	// > @3@4@5@6
	// >
	// > If set, it defines which node labels to add to the event, others will be ignored.
	AllowedNodeLabels  []string `json:"allowed_node_labels" slice:"true"` // *
	AllowedNodeLabels_ map[string]bool

	// > @3@4@5@6
	// >
	// > Skips retrieving Kubernetes meta information using Kubernetes API and adds only `k8s_node` field.
	OnlyNode bool `json:"only_node" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Kubernetes dir with container logs. It's like `watching_dir` parameter from [file plugin](/plugin/input/file/README.md) config.
	WatchingDir  string `json:"watching_dir" default:"/var/log/containers"` // *
	WatchingDir_ string

	// > @3@4@5@6
	// >
	// > The filename to store offsets of processed files. It's like `offsets_file` parameter from [file plugin](/plugin/input/file/README.md) config.
	OffsetsFile string `json:"offsets_file" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Under the hood this plugin uses [file plugin](/plugin/input/file/README.md) to collect logs from files. So you can change any [file plugin](/plugin/input/file/README.md) config parameter using `file_config` section. Check out an example.
	FileConfig file.Config `json:"file_config" child:"true"` // *

	// > @3@4@5@6
	// >
	// > K8sMeta params
	// >
	// > Add meta information to an event (look at Meta params)
	// > Use [go-template](https://pkg.go.dev/text/template) syntax
	// >
	// > Built-in meta params
	// >
	// > `k8s_pod`: `{{ .pod_name }}`
	// >
	// > `k8s_namespace`: `{{ .namespace_name }}`
	// >
	// > `k8s_container`: `{{ .container }}`
	// >
	// > `k8s_container_id`: `{{ .container_id }}`
	// >
	// > Example: ```component: '{{ index .pod.Labels "component" | default .k8s_container }}'```
	K8sMeta cfg.MetaTemplates `json:"meta"` // *
}

var startCounter atomic.Int32

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:              "k8s",
		Factory:           Factory,
		AdditionalActions: []string{"k8s-multiline"},

		Endpoints: map[string]func(http.ResponseWriter, *http.Request){
			"reset": file.ResetterRegistryInstance.Reset,
			"info":  file.InfoRegistryInstance.Info,
		},
	})
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "k8s-multiline",
		Factory: MultilineActionFactory,
	})
}

func MultilineActionFactory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &MultilineAction{}, &Config{}
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{fp: &file.Plugin{}}, &Config{}
}

// Start plugin.
func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.logger = params.Logger
	p.params = params
	p.config = config.(*Config)

	startCounter := startCounter.Inc()

	if startCounter == 1 {
		meta.DeletedPodsCacheSize = p.config.DeletedPodsCacheSize
		meta.EnableGatherer(p.logger)
	}

	if meta.CriType == "docker" {
		p.params.Controller.SuggestDecoder(decoder.JSON)
	} else {
		p.params.Controller.SuggestDecoder(decoder.CRI)
	}

	metaConfig := cfg.MetaTemplates{}
	if p.config.K8sMeta != nil {
		metaConfig = p.config.K8sMeta
	}

	fileMeta := cfg.MetaTemplates{}
	if p.config.FileConfig.Meta != nil {
		fileMeta = p.config.FileConfig.Meta
	}

	if !p.config.OnlyNode {
		setBuiltInMeta(fileMeta)
	}

	for k, v := range metaConfig {
		fileMeta[k] = v
	}
	p.config.FileConfig.Meta = fileMeta

	p.fp.Start(&p.config.FileConfig, params)
}

func setBuiltInMeta(metaConfig cfg.MetaTemplates) {
	metaConfig["k8s_pod"] = "{{ .pod_name }}"
	metaConfig["k8s_namespace"] = "{{ .namespace }}"
	metaConfig["k8s_container"] = "{{ .container_name }}"
	metaConfig["k8s_container_id"] = "{{ .container_id }}"
}

/*{ meta-params
**`pod_name`** - string

**`namespace`** - string

**`container_name`** - string

**`container_id`** - string

**`pod`** - k8s.io/api/core/v1.Pod
}*/

// Commit event.
func (p *Plugin) Commit(event *pipeline.Event) {
	p.fp.Commit(event)
}

// Stop plugin work.
func (p *Plugin) Stop() {
	p.fp.Stop()
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	return p.fp.PassEvent(event)
}
