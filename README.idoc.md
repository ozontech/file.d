![file.d](/static/file.d.png)

# Overview
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/ozontech/file.d/graphs/commit-activity)
[![CI](https://github.com/ozontech/file.d/actions/workflows/ci.yml/badge.svg)](https://github.com/ozontech/file.d/actions/workflows/go.yml)
[![Code coverage](https://codecov.io/github/ozontech/file.d/coverage.svg?branch=master)](https://codecov.io/github/ozontech/file.d?branch=master)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/ozontech/file.d)](https://github.com/ozontech/file.d)
[![GoReportCard example](https://goreportcard.com/badge/github.com/ozontech/file.d)](https://goreportcard.com/report/github.com/ozontech/file.d)

`file.d` is a blazing fast tool for building data pipelines: read, process, and output events. Primarily developed to read from files, but also supports numerous input/action/output plugins. 

> ⚠ Although we use it in production, `it still isn't v1.0.0`. Please, test your pipelines carefully on dev/stage environments.  

## Contributing
`file.d` is an open-source project and contributions are very welcome!
Please make sure to read our [contributing guide](/CONTRIBUTING.md) before creating an issue and opening a PR!

## Motivation
Well, we already have several similar tools: vector, filebeat, logstash, fluend-d, fluent-bit, etc.

Performance tests state that best ones achieve a throughput of roughly 100MB/sec. 
Guys, it's 2023 now. HDDs and NICs can handle the throughput of a **few GB/sec** and CPUs processes **dozens of GB/sec**. Are you sure **100MB/sec** is what we deserve? Are you sure it is fast?

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

**Input**: @global-contents-table-plugin-input|links

**Action**: @global-contents-table-plugin-action|links

**Output**: @global-contents-table-plugin-output|links


## What's next
* [Quick start](/docs/quick-start.md)
* [Installation](/docs/installation.md)
* [Examples](/docs/examples.md)
* [Configuring](/docs/configuring.md)
* [Architecture](/docs/architecture.md)
* [Testing](/docs/testing.md)
* [Monitoring](/docs/monitoring.md)
* [Contributing](/CONTRIBUTING.md)
* [License](/docs/license.md)

Join our community in Telegram: https://t.me/file_d_community