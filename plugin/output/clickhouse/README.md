# clickhouse output
It sends the event batches to Clickhouse database using
[Native format](https://clickhouse.com/docs/en/interfaces/formats/#native) and
[Native protocol](https://clickhouse.com/docs/en/interfaces/tcp/).

File.d uses low level Go client - [ch-go](https://github.com/ClickHouse/ch-go) to provide these features.

### Config params
**`addresses`** *`[]string`* *`required`* 

TCP Clickhouse addresses, e.g.: 127.0.0.1:9000.
Check the insert_strategy to find out how File.d will behave with a list of addresses.

<br>

**`insert_strategy`** *`string`* *`default=round_robin`* *`options=round_robin|in_order`* 

If more than one addresses are set, File.d will insert batches depends on the strategy:
round_robin - File.d will send requests in the round-robin order.
in_order - File.d will send requests starting from the first address, ending with the number of retries.

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
If you set 128-256 bits - File.d will cast the number to the int64.
* DateTime, DateTime64
* String
* Enum8, Enum16
* Bool
* Nullable
* IPv4, IPv6
* LowCardinality(String)
* Array(String)
If you need more types, please, create an issue.

<br>

**`strict_types`** *`bool`* *`default=true`* 

If true, file.d will fail when types are mismatched.

If false, file.d will cast any JSON type to the column type.

For example, if strict_types is false and an event value is a Number,
but the column type is a Bool, the Number will be converted to the "true"
if the value is "1".
But if the value is an Object and the column is an Int
File.d converts the Object to "0" to prevent fall.

In the non-strict mode, for String and Array(String) columns the value will be encoded to JSON.

If the strict mode is enabled file.d fails (exit with code 1) in above examples.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code.

<br>

**`clickhouse_settings`** *`Settings`* 

Additional settings to the Clickhouse.
Settings list: https://clickhouse.com/docs/en/operations/settings/settings

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Retention milliseconds for retry to DB.

<br>

**`insert_timeout`** *`cfg.Duration`* *`default=10s`* 

Timeout for each insert request.

<br>

**`max_conns`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

Max connections in the connection pool.

<br>

**`min_conns`** *`cfg.Expression`* *`default=gomaxprocs*1`* 

Min connections in the connection pool.

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