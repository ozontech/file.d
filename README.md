# file-d
File-d is a daemon which allows you to build data pipelines: read, process and output events. Similar tools are: `vector`, `filebeat`, `logstash`, `fluend-d`, `fluent-bit`.

## Main features
* Extremely fast: zero allocations on hot paths, insane-json
* Predictable: it uses event pooling, so memory consumption is limited 
* Container / cloud / kubernetes native
* Simply configurable with YAML
* Prometheus-friendly: transform your events into metrics on any pipeline stage
* Well tested and used in production to collect logs from kubernetes cluster with 4500+ total CPU cores
* Don't loose any data due to commitment mechanism

## Performance
`file-d` achieve `900Mb/s` throughput on regular MacBook Pro 2017 with two physical cores in `file > json decode > devnull` case. 


### CPU

### Memory consumption

## Installation

## Guarantees

## Plugins

## Motivation