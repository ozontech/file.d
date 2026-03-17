# Socket plugin
It sends events to a socket endpoint.
Supports TCP, UDP, and Unix socket protocols.

Events are sent in batches serialized as newline-delimited JSON, compatible with the socket input plugin.
If a network error occurs, the batch will be retried according to the backoff settings.

Supports [dead queue](/plugin/output/README.md#dead-queue).

## Examples
TCP:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: tcp
      address: ':6666'
    ...
```
---
TLS:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: tcp
      address: ':6666'
      ca_cert: './client.pem'
      private_key: './client.key'
    ...
```
---
UDP:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: udp
      address: '[2001:db8::1]:1234'
    ...
```
---
Unix:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: unix
      address: '/tmp/filed.sock'
    ...
```

## Config params
**`network`** *`string`* *`default=tcp`* *`options=tcp|udp|unix`* 

Which network protocol to use for the outgoing connection.

<br>

**`address`** *`string`* *`required`* 

Remote address to connect to.

Examples:
- 1.2.3.4:6666
- :6666
- /tmp/filed.sock

<br>

**`ca_cert`** *`string`* 

Client certificate in PEM encoding. This can be a path or the contents of the file.
> Enables mutual TLS. Works only when `network` is `tcp`.

<br>

**`private_key`** *`string`* 

Client private key in PEM encoding. This can be a path or the contents of the file.
> Enables mutual TLS. Works only when `network` is `tcp`.

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

**`dial_timeout`** *`cfg.Duration`* *`default=5s`* 

It defines how much time to wait for the connection.

<br>

**`write_timeout`** *`cfg.Duration`* *`default=5s`* 

Timeout for writing a single batch to the socket.
> Set to `0` to disable.

<br>

**`maintenance_interval`** *`cfg.Duration`* *`default=1m`* 

It defines how often to perform maintenance

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

It defines how many workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

A maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't full.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to socket.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*