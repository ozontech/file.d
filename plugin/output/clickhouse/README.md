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

**`table`** *`string`* *`required`* 

Clickhouse database name to search the table.
*`string`* *`required`* 

Clickhouse target table.

<br>

**`retry`** *`int`* *`default=10`* 

Table schema to use [Native format](https://clickhouse.com/docs/en/interfaces/formats/#native).
*`int`* *`default=10`* 

The level of the Compression.
Disabled - lowest CPU overhead.
LZ4 - medium CPU overhead.
ZSTD - high CPU overhead.
None - uses no compression but data has checksums.
*`int`* *`default=10`* 

Retries of insertion. If file.d cannot insert for this number of attempts,
file.d will fall with non-zero exit code.

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Allowing Clickhouse to discard extra data.
If disabled and extra data found, Clickhouse throws an error and file.d will infinitely retry invalid requests.
If you want to disable the settings, check the `keep_fields` plugin to prevent the appearance of extra data.
*`cfg.Duration`* *`default=50ms`* 

Additional settings to the Clickhouse.
Settings list: https://clickhouse.com/docs/en/operations/settings/settings
*`cfg.Duration`* *`default=50ms`* 

Retention milliseconds for retry to DB.

<br>

**`db_request_timeout`** *`cfg.Duration`* *`default=3000ms`* 

Timeout for DB requests in milliseconds.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How much workers will be instantiated to send batches.

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