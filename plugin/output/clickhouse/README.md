# clickhouse output
It sends the event batches to Clickhouse database using
[Native format](https://clickhouse.com/docs/en/interfaces/formats/#native) and
[Native protocol](https://clickhouse.com/docs/en/interfaces/tcp/).

File.d uses low level Go client - [ch-go](https://github.com/ClickHouse/ch-go) to provide these features.

### Config params
**`address`** *`string`* *`required`* 

TCP Clickhouse address, e.g. 127.0.0.1:9000.

<br>

**`ca_cert`** *`string`* 

CA certificate in PEM encoding. This can be a path or the content of the certificate.

<br>

**`database`** *`string`* *`default=default`* 

Clickhouse database name to search the table.

<br>

**`user`** *`string`* *`default=default`* 

Clickhouse database user.

<br>

**`password`** *`string`* 

Clickhouse database password.

<br>

**`quota_key`** *`string`* 

Clickhouse quota key.
https://clickhouse.com/docs/en/operations/quotas

<br>

**`table`** *`string`* *`required`* 

Clickhouse target table.

<br>

**`columns`** *`[]Column`* *`required`* 

Clickhouse table columns. Each column must contain `name` and `type`.
File.d supports next data types:
* Signed and unsigned integers from 8 to 64 bits.
If you set 128-256 bits - file.d will cast the number to the int64.
* DateTime, DateTime64
* String
* Enum8, Enum16
* Bool
* Nullable
* IPv4, IPv6
If you need more types, please, create an issue.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If file.d cannot insert for this number of attempts,
file.d will fall with non-zero exit code.

<br>

**`clickhouse_settings`** *`Settings`* 

Additional settings to the Clickhouse.
Settings list: https://clickhouse.com/docs/en/operations/settings/settings

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Retention milliseconds for retry to DB.

<br>

**`db_request_timeout`** *`cfg.Duration`* *`default=10s`* 

Timeout for DB requests in milliseconds.

<br>

**`max_conn_lifetime`** *`cfg.Duration`* *`default=30m`* 

How long a connection lives before it is killed and recreated.

<br>

**`max_conn_idle_time`** *`cfg.Duration`* *`default=5m`* 

How long an unused connection lives before it is killed.

<br>

**`health_check_period`** *`cfg.Duration`* *`default=1m`* 

How often to check that idle connections is time to kill.

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


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*