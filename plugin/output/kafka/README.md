# Kafka output
It sends the event batches to kafka brokers using `franz-go` lib.

### Config params
**`brokers`** *`[]string`* *`required`* 

List of kafka brokers to write to.

<br>

**`default_topic`** *`string`* *`required`* 

The default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.

<br>

**`client_id`** *`string`* *`default=file-d`* 

Kafka client ID.

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

**`max_message_bytes`** *`cfg.Expression`* *`default=1000000`* 

The maximum permitted size of a message.
Should be set equal to or smaller than the broker's `message.max.bytes`.

<br>

**`compression`** *`string`* *`default=none`* *`options=none|gzip|snappy|lz4|zstd`* 

Compression codec

<br>

**`ack`** *`string`* *`default=leader`* *`options=no|leader|all-isr`* 

Required acks for produced records

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not
**Experimental feature**

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Retention milliseconds for retry.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>

**`is_sasl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`sasl_mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512`* 

SASL mechanism to use.

<br>

**`sasl_username`** *`string`* *`default=user`* 

SASL username.

<br>

**`sasl_password`** *`string`* *`default=password`* 

SASL password.

<br>

**`is_ssl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SSL/TLS connections method.

<br>

**`ssl_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will skip SSL/TLS verification.

<br>

**`client_cert`** *`string`* 

Path or content of a PEM-encoded client certificate file.

<br>

**`client_key`** *`string`* 

> Path or content of a PEM-encoded client key file.

<br>

**`ca_cert`** *`string`* 

Path or content of a PEM-encoded CA file.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*