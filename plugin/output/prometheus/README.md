# Prometheus output
It sends metrics to Prometheus using the remote write API. The plugin receives metric events from the pipeline (e.g., from the event_to_metrics action plugin) and forwards them to a Prometheus-compatible endpoint.

### Config params
**`endpoint`** *`string`* *`default=http://localhost:9090/api/v1/write`* *`required`* 

Prometheus remote write endpoint URL.

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

Client timeout when sends requests to Prometheus HTTP API.

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

**`retry`** *`int`* *`default=10`* 

Retries of upload. If File.d cannot upload for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to upload.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>

**`attempt_num`** *`int`* *`default=3`* 

Number of retry attempts.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*