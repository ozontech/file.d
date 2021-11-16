![file.d](/static/file.d.png)

# Overview
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/ozonru/file.d/graphs/commit-activity)
[![CI](https://github.com/Snyssfx/file.d/actions/workflows/go.yml/badge.svg)](https://github.com/Snyssfx/file.d/actions/workflows/go.yml)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/ozonru/file.d)](https://github.com/ozonru/file.d)
[![GoReportCard example](https://goreportcard.com/badge/github.com/ozonru/file.d)](https://goreportcard.com/report/github.com/ozonru/file.d)

`file.d` is a blazing fast tool for building data pipelines: read, process, and output events. Primarily developed to read from files, but also supports numerous input/action/output plugins. 

> ⚠ Although we use it in production, `it still isn't v1.0.0`. Please, test your pipelines carefully on dev/stage environments.  

## Motivation
Well, we already have several similar tools: vector, filebeat, logstash, fluend-d, fluent-bit, etc.

Performance tests state that best ones achieve a throughput of roughly 100MB/sec. 
Guys, it's 2020 now. HDDs and NICs can handle the throughput of a **few GB/sec** and CPUs processes **dozens of GB/sec**. Are you sure **100MB/sec** is what we deserve? Are you sure it is fast?

## Main features
* Fast: more than 10x faster compared to similar tools
* Predictable: it uses pooling, so memory consumption is limited
* Reliable: doesn't lose data due to commitment mechanism
* Container / cloud / kubernetes native
* Simply configurable with YAML
* Prometheus-friendly: transform your events into metrics on any pipeline stage
* Vault-friendly: store sensitive info and get it for any pipeline parameter
* Well-tested and used in production to collect logs from Kubernetes cluster with 3000+ total CPU cores

## Performance
On MacBook Pro 2017 with two physical cores `file.d` can achieve the following throughput:
* 1.7GB/s in `files > devnull` case
* 1.0GB/s in `files > json decode > devnull` case

TBD: throughput on production servers.  

## Plugins

**Input**: [dmesg](plugin/input/dmesg/README.md), [fake](plugin/input/fake/README.md), [file](plugin/input/file/README.md), [http](plugin/input/http/README.md), [journalctl](plugin/input/journalctl/README.md), [k8s](plugin/input/k8s/README.md), [kafka](plugin/input/kafka/README.md)

**Action**: [add_host](plugin/action/add_host/README.md), [convert_date](plugin/action/convert_date/README.md), [debug](plugin/action/debug/README.md), [discard](plugin/action/discard/README.md), [flatten](plugin/action/flatten/README.md), [join](plugin/action/join/README.md), [json_decode](plugin/action/json_decode/README.md), [keep_fields](plugin/action/keep_fields/README.md), [modify](plugin/action/modify/README.md), [parse_es](plugin/action/parse_es/README.md), [parse_re2](plugin/action/parse_re2/README.md), [remove_fields](plugin/action/remove_fields/README.md), [rename](plugin/action/rename/README.md), [throttle](plugin/action/throttle/README.md)

**Output**: [devnull](plugin/output/devnull/README.md), [elasticsearch](plugin/output/elasticsearch/README.md), [gelf](plugin/output/gelf/README.md), [kafka](plugin/output/kafka/README.md), [splunk](plugin/output/splunk/README.md), [stdout](plugin/output/stdout/README.md)

## What's next
* [Quick start](/docs/quick-start.md)
* [Installation](/docs/installation.md)
* [Examples](/docs/examples.md)
* [Configuring](/docs/configuring.md)
* [Architecture](/docs/architecture.md)
* [Contributing](/docs/contributing.md)
* [License](/docs/license.md)

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*
