# Kafka output
It sends the event batches to kafka brokers using `sarama` lib.

### Config params
**`brokers`** *`[]string`* *`required`* 

List of kafka brokers to write to.

<br>

**`default_topic`** *`string`* *`required`* 

The default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.

<br>

**`use_topic_field`** *`bool`* *`default=false`* 

If set, the plugin will use topic name from the event field.

<br>

**`topic_field`** *`string`* *`default=topic`* 

Which event field to use as topic name. It works only if `should_use_topic_field` is set.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How many workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

A maximum quantity of the events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout the batch will be sent even if batch isn't full.

<br>

**`is_sasl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`sasl_username`** *`string`* *`default=user`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`sasl_password`** *`string`* *`default=password`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`is_ssl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SSL connections method.

<br>

**`ssl_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will use skip SSL verification.

<br>

**`pem_file`** *`string`* *`default=/file.d/certs`* 

If SaslSslEnabled, the plugin will use path to the PEM certificate.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*