# Kubernetes plugin
Adds kubernetes meta information into events collected from docker log files. Also joins split docker logs into one event.

Source docker log file name should be in format:<br> `[pod-name]_[namespace]_[container-name]-[container-id].log` 

E.g. `my_pod-1566485760-trtrq_my-namespace_my-container-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

Information which plugin adds: 
* `k8s_node` – node name where pod is running
* `k8s_pod` – pod name
* `k8s_namespace` – pod namespace name
* `k8s_container` – pod container name
* `k8s_label_*` – pod labels


### Config params
**`max_event_size`** *`int`* *`default=1000000`* 

Docker splits long logs by 16kb chunks. Plugin joins them back, but if event will be longer than this value in bytes it will be split after all.
> Because of optimization it's not strict rule. Events may be split even if they won't gonna exceed the limit.

<br>

**`labels_whitelist`** *`[]string`* 

If set defines which pod labels to add to the event, others will be ignored.

<br>

**`only_node`** *`bool`* *`default=false`* 

Skip retrieving k8s meta information using kubernetes API and add only `k8s_node` field.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*