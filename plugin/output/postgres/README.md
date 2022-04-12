# postgres output
It sends the event batches to postgres db using pgx.

### Config params
**`strict`** *`bool`* *`default=false`* 

In strict mode file.d will crash on events without required columns.

<br>

**`host`** *`string`* *`required`* 

DB host.

<br>

**`port`** *`uint16`* *`required`* 

Db port.

<br>

**`dbname`** *`string`* *`required`* 

Dbname in pg.

<br>

**`user`** *`string`* *`required`* 

Pg user name.

<br>

**`password`** *`string`* *`required`* 

Pg user pass.

<br>

**`table`** *`string`* *`required`* 

Pg target table.

<br>

**`columns`** *`[]ConfigColumn`* *`required`* 

Array of DB columns. Each column have:
name, type (int, string, timestamp - which int that will be converted to timestamptz of rfc3339)
and nullable options.

<br>

**`retry`** *`int`* *`default=3`* 

Retries of insertion.

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Retention milliseconds for retry to DB.

<br>

**`db_request_timeout`** *`cfg.Duration`* *`default=3000ms`* 

Timeout for DB requests in milliseconds.

<br>

**`db_health_check_period`** *`cfg.Duration`* *`default=60s`* 

Timeout for DB health check.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How much workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

Maximum quantity of events to pack into one batch.

<br>

**`batch_size`** 
<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't completed.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*