# Kubernetes plugin
Plugin adds k8s meta info to docker logs and also joins split docker logs into one event.
Source docker log file name should be in format: `[pod-name]_[namespace]_[container-name]-[container-id].log` e.g. `/docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

## Config params
### max_event_size

`int` `default=1000000`  

Docker splits long logs by 16kb chunks. Plugin joins them back, but if event will be longer than this value in bytes it will be split after all.
> Because of optimization it's not strict rule. Events may be split even if they won't gonna exceed the limit.

### labels_whitelist

`string`   

By default plugin adds all pod labels to the event. List here only those which are needed.
e.g. `app,release`

### only_node

`bool` `default=false`  

Skip retrieving k8s meta information using k8s API and add only `k8s_node` field.



 Generated using *insane-doc*