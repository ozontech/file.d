# Kubernetes plugin
It reads Kubernetes logs and also adds pod meta-information. Also, it joins split logs into a single event.

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

### Config params
**`split_event_size`** *`int`* *`default=1000000`* 

Docker splits long logs by 16kb chunks. The plugin joins them back, but if an event is longer than this value in bytes, it will be split after all.
> Due to the optimization process it's not a strict rule. Events may be split even if they won't exceed the limit.

<br>

**`allowed_pod_labels`** *`[]string`* 

If set, it defines which pod labels to add to the event, others will be ignored.

<br>

**`allowed_node_labels`** *`[]string`* 

If set, it defines which node labels to add to the event, others will be ignored.

<br>

**`only_node`** *`bool`* *`default=false`* 

Skips retrieving Kubernetes meta information using Kubernetes API and adds only `k8s_node` field.

<br>

**`watching_dir`** *`string`* *`default=/var/log/containers`* 

Kubernetes dir with container logs. It's like `watching_dir` parameter from [file plugin](/plugin/input/file/README.md) config.

<br>

**`offsets_file`** *`string`* *`required`* 

The filename to store offsets of processed files. It's like `offsets_file` parameter from [file plugin](/plugin/input/file/README.md) config.

<br>

**`file_config`** *`file.Config`* 

Under the hood this plugin uses [file plugin](/plugin/input/file/README.md) to collect logs from files. So you can change any [file plugin](/plugin/input/file/README.md) config parameter using `file_config` section. Check out an example.

<br>

**`meta`** *`cfg.MetaTemplates`* 

Meta params

Add meta information to an event (look at Meta params)
Use [go-template](https://pkg.go.dev/text/template) syntax

<br>


### Meta params
**`pod`** 

**`namespace`** 

**`container`** 

**`container_id`** 

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*