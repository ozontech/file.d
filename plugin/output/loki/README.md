# Loki output
It sends the logs batches to Loki using HTTP API.

### Config params
**`address`** *`string`* *`required`* 

A full URI address of Loki

Example address

http://127.0.0.1:3100 or https://loki:3100

<br>

**`auth_enabled`** *`bool`* *`default=false`* 

Array of labels to send logs

Example labels

label=value
*`bool`* *`default=false`* 

Authorization enabled, if true set OrgID

<br>

**`org_id`** *`string`* 

Authorization enabled, if set true set OrgID

Example organization id

example-org

<br>

**`tls_enabled`** *`bool`* *`default=false`* 

If set true, the plugin will use SSL/TLS connections method.

<br>

**`tls_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will skip SSL/TLS verification.

<br>

**`client_cert`** *`string`* 

Path or content of a PEM-encoded client certificate file.

<br>

**`client_key`** *`string`* 

> Path or content of a PEM-encoded client key file.

<br>

**`ca_cert`** *`string`* 

Path or content of a PEM-encoded CA file. This can be a path or the content of the certificate.

<br>

**`request_timeout`** *`cfg.Duration`* *`default=1s`* 

Client timeout when sends requests to Loki HTTP API.

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

After an insert error, fall with a non-zero exit code or not
**Experimental feature**

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*
