# Output plugins

## devnull
Provides API to test pipelines and other plugins.

[More details...](plugin/output/devnull/README.md)
## elasticsearch
Plugin writes events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs batch will be infinitely tries to be delivered to random endpoint.

[More details...](plugin/output/elasticsearch/README.md)
## gelf
Plugin sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be grater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version` *`string=1.1`*
* `host` *`string`*
* `short_message` *`string`*
* `full_message` *`string`*
* `timestamp` *`number`*
* `level` *`number`*
* `_extra_field_1` *`string`*
* `_extra_field_2` *`string`*
* `_extra_field_3` *`string`*

Every field with an underscore prefix `_` will be treated as an extra field.
Allowed characters in a field names are letters, numbers, underscores, dashes and dots.

[More details...](plugin/output/gelf/README.md)
## kafka
Sends event batches to kafka brokers using `sarama` lib.

[More details...](plugin/output/kafka/README.md)
## stdout
Plugin simply writes events to stdout(also known as console).

[More details...](plugin/output/stdout/README.md)
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*