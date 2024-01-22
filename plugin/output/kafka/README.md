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
> `deprecated` Use `sasl.enabled` instead.

<br>

**`sasl_mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512`* 

SASL mechanism to use.
> `deprecated` Use `sasl.mechanism` instead.

<br>

**`sasl_username`** *`string`* *`default=user`* 

SASL username.
> `deprecated` Use `sasl.username` instead.

<br>

**`sasl_password`** *`string`* *`default=password`* 

SASL password.
> `deprecated` Use `sasl.password` instead.

<br>

**`is_ssl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SSL/TLS connections method.
> `deprecated` Use `tls.enabled` instead.

<br>

**`ssl_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will use skip SSL/TLS verification.
> `deprecated` Use `tls.skip_verify` instead.

<br>

**`pem_file`** *`string`* *`default=/file.d/certs`* 

Path or content of a PEM-encoded CA file.
> `deprecated` Use `tls.ca_cert` instead.

<br>

**`sasl`** *`SASLConfig`* 

SASL config.
Disabled by default.
See `SASLConfig` for details.

<br>

**`tls`** *`TLSConfig`* 

TLS config.
Disabled by default.
See `TLSConfig` for details.

<br>


#### SASL config
**`enabled`** *`bool`* *`default=false`*

If set, the plugin will use SASL authentications mechanism.

<br>

**`mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512`*

SASL mechanism to use.

<br>

**`username`** *`string`* *`default=user`*

SASL username.

<br>

**`password`** *`string`* *`default=password`*

SASL password.

<br>

#### TLS config
**`enabled`** *`bool`* *`default=false`*

If set, the plugin will use SSL/TLS connections method.

<br>

**`skip_verify`** *`bool`* *`default=false`*

If set, the plugin will skip verification.

<br>

**`ca_cert`** *`string`* *`default=/file.d/certs`*

Path or content of a PEM-encoded CA file.

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*