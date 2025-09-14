# Loki output
It sends the logs batches to Loki using HTTP API.

Supports [dead queue](/plugin/output/README.md#dead-queue).

### Config params
**`address`** *`string`* *`required`* 

A full URI address of Loki

Example address

http://127.0.0.1:3100 or https://loki:3100

<br>

**`labels`** *`[]Label`* 

Array of labels to send logs

Example labels

label=value

<br>

**`message_field`** *`string`* *`required`* 

Message field from log to be mapped to loki

Example

message

<br>

**`data_format`** *`string`* *`default=json`* *`options=json|proto`* 

Sendoing data format to Loki.

By default sending data format is json.
* if `json` is provided plugin will send logs in json format.
* if `proto` is provided plugin will send logs in Snappy-compressed protobuf format.

<br>

**`timestamp_field`** *`string`* *`required`* 

Timestamp field from log to be mapped to loki

Example

timestamp

<br>

**`auth`** *`AuthConfig`* 

Auth config.

`AuthConfig` params:
* `strategy` describes strategy to use; options:"disabled|tenant|basic|bearer"
By default strategy is `disabled`.
* `tenant_id` should be provided if strategy is `tenant`.
* `username` should be provided if strategy is `basic`.
Username is used for HTTP Basic Authentication.
* `password` should be provided if strategy is `basic`.
Password is used for HTTP Basic Authentication.
* `bearer_token` should be provided if strategy is `bearer`.
Token is used for HTTP Bearer Authentication.

<br>

**`tls_enabled`** *`bool`* *`default=false`* 

If set true, the plugin will use SSL/TLS connections method.

<br>

**`tls_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will skip SSL/TLS verification.

<br>

**`request_timeout`** *`cfg.Duration`* *`default=1s`* 

Client timeout when sends requests to Loki HTTP API.

<br>

**`connection_timeout`** *`cfg.Duration`* *`default=5s`* 

It defines how much time to wait for the connection.

<br>

**`keep_alive`** *`KeepAliveConfig`* 

Keep-alive config.

`KeepAliveConfig` params:
* `max_idle_conn_duration` - idle keep-alive connections are closed after this duration.
By default idle connections are closed after `10s`.
* `max_conn_duration` - keep-alive connections are closed after this duration.
If set to `0` - connection duration is unlimited.
By default connection duration is `5m`.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How much workers will be instantiated to send batches.
It also configures the amount of minimum and maximum number of database connections.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

Maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't completed.

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to Loki.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*