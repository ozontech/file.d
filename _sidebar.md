- **Getting started**
  - [Overview](/README.md)
  - [Quick start](/docs/quick-start.md)
  - [Installation](/docs/installation.md)
  - [Examples](/docs/examples.md)
  - [Configuring](/docs/configuring.md)

- **Documentation**
  - [Architecture](/docs/architecture.md)
  - [Benchmarks](/docs/benchmarks.md)
  - [Guarantees](/docs/guarantees.md)
  - [Optimization tips](/docs/optimization-tips.md)

- **Plugins**
  - Input
    - [dmesg](plugin/input/dmesg/README.md)
    - [fake](plugin/input/fake/README.md)
    - [file](plugin/input/file/README.md)
    - [http](plugin/input/http/README.md)
    - [journalctl](plugin/input/journalctl/README.md)
    - [k8s](plugin/input/k8s/README.md)
    - [kafka](plugin/input/kafka/README.md)

  - Action
    - [add_file_name](plugin/action/add_file_name/README.md)
    - [add_host](plugin/action/add_host/README.md)
    - [convert_date](plugin/action/convert_date/README.md)
    - [convert_log_level](plugin/action/convert_log_level/README.md)
    - [convert_utf8_bytes](plugin/action/convert_utf8_bytes/README.md)
    - [debug](plugin/action/debug/README.md)
    - [discard](plugin/action/discard/README.md)
    - [flatten](plugin/action/flatten/README.md)
    - [join](plugin/action/join/README.md)
    - [join_template](plugin/action/join_template/README.md)
    - [json_decode](plugin/action/json_decode/README.md)
    - [json_encode](plugin/action/json_encode/README.md)
    - [json_extract](plugin/action/json_extract/README.md)
    - [keep_fields](plugin/action/keep_fields/README.md)
    - [mask](plugin/action/mask/README.md)
    - [metric](plugin/action/metric/README.md)
    - [modify](plugin/action/modify/README.md)
    - [move](plugin/action/move/README.md)
    - [parse_es](plugin/action/parse_es/README.md)
    - [parse_re2](plugin/action/parse_re2/README.md)
    - [remove_fields](plugin/action/remove_fields/README.md)
    - [rename](plugin/action/rename/README.md)
    - [set_time](plugin/action/set_time/README.md)
    - [split](plugin/action/split/README.md)
    - [throttle](plugin/action/throttle/README.md)

  - Output
    - [clickhouse](plugin/output/clickhouse/README.md)
    - [devnull](plugin/output/devnull/README.md)
    - [elasticsearch](plugin/output/elasticsearch/README.md)
    - [file](plugin/output/file/README.md)
    - [gelf](plugin/output/gelf/README.md)
    - [kafka](plugin/output/kafka/README.md)
    - [postgres](plugin/output/postgres/README.md)
    - [s3](plugin/output/s3/README.md)
    - [splunk](plugin/output/splunk/README.md)
    - [stdout](plugin/output/stdout/README.md)


- **Pipeline**
  - [Match modes](pipeline/README.md#match-modes)
  - [Experimental: Do If rules](pipeline/doif/README.md#experimental-do-if-rules)

- **Other**
  - [Contributing](/docs/contributing.md)
  - [License](/docs/license.md)