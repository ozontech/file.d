# File-d

## What is it
File-d is a daemon which allows you to build data pipelines: read, process and output events. 

## Motivation
Well, we already have a number of similar tools: `vector`, `filebeat`, `logstash`, `fluend-d`, `fluent-bit`, etc.

Performance tests states that best ones achieve around `100MB/sec` throughput. 
Guys, its 2020 now. Hard disks and network interfaces can read data at rate of a `few GB/sec`. 
And CPUs can process `dozens of GB/sec`. Are you sure `100MB/sec` is what we deserve? Are you sure `100MB/sec` can be called "fast"?

## Main features
* More than 10x faster compared to the similar tools
* Predictable: it uses pooling, so memory consumption is limited 
* Container / cloud / kubernetes native
* Simply configurable with YAML
* Prometheus-friendly: transform your events into metrics on any pipeline stage
* Well tested and used in production to collect logs from kubernetes cluster with 4500+ total CPU cores
* Don't loose any data due to commitment mechanism

## Performance
On MacBook Pro 2017 with two physical cores `file-d` can achieve throughput:
* `1.7GB/s` in `files > devnull` case
* `1GB/s` in `files > json decode > devnull` case

### Benchmarks
To be filled

### Optimization tips
* Limit regular expressions usage if you care about CPU load.
* File input plugin must have at least X files to process data efficiently. Where X is number of CPU cores.
* Plugin parameters such as `worker_count`, `batch_size` may have huge impact on throughput. Tune them for your system and needs.
* Pipeline parameter `capacity` is also highly tunable. Bigger sizes increase performance, but increase memory usage too.
* Memory usage is highly depends on maximum event size. In the examples below it's assumed that `event_size` is an maximum event size which can get into pipeline.       
* Main RAM consumers are output buffers. Rough estimation is `worker_count * batch_size * event_size`. For `worker_count=16`, `batch_size=256`, `event_size=64KB` output buffers will take `16*256*64KB=256MB`.
* Next significant RAM consumer an event pool. Rough estimation is `capacity * event_size`. So if you have pipeline with `capacity=1024` and `event_size=64KB`, then event pool size will be `1024*64KB=64MB`.
* For file input plugin buffers takes `worker_count*read_buffer_size` of RAM which is `2MB`, if `worker_count=16` and `read_buffer_size=128KB`.
* Total estimation of RAM usage is `input_buffers+event_pool+output_buffers`, which is `64MB+256MB+2MB=322MB` for examples above.

## Installation
To be filled

## Guarantees

## Plugins

**Input**: @global-contents-table-plugin-input|links

**Action**: @global-contents-table-plugin-action|links

**Output**: @global-contents-table-plugin-output|links