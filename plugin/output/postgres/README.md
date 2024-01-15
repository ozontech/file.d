# Postgres output
It sends the event batches to postgres db using pgx.

### Config params
**`strict`** *`bool`* *`default=false`* 

Deprecated. Use `strict_fields` flag instead.

<br>

**`strict_fields`** *`bool`* *`default=false`* 

In strict mode file.d will crash on events without required fields.

<br>

**`conn_string`** *`string`* *`required`* 

PostgreSQL connection string in URL or DSN format.

Example DSN:

`user=user password=secret host=pg.example.com port=5432 dbname=mydb sslmode=disable pool_max_conns=10`

<br>

**`table`** *`string`* *`required`* 

Pg target table.

<br>

**`columns`** *`[]ConfigColumn`* *`required`* 

Array of DB columns. Each column have:
name, type (int, string, timestamp - which int that will be converted to timestamptz of rfc3339)
and nullable options.

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

Retention milliseconds for retry to DB.

<br>

**`db_request_timeout`** *`cfg.Duration`* *`default=3000ms`* 

Multiplier for exponential increase of retention between retries
*`cfg.Duration`* *`default=3000ms`* 

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

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't completed.

<br>


### Example
**Example**
Postgres output example:
```yaml
pipelines:
  example_pipeline:
    input:
      type: file
      persistence_mode: async
      watching_dir: ./
      filename_pattern: input_example.json
      offsets_file: ./offsets.yaml
      offsets_op: reset
	output:
      type: postgres
      conn_string: "user=postgres host=localhost port=5432 dbname=postgres sslmode=disable pool_max_conns=10"
      table: events
      columns:
        - name: id
          type: int
        - name: name
          type: string
      retry: 10
      retention: 1s
      retention_exponentially_multiplier: 1.5
```

input_example.json
```json
{"id":1,"name":"name1"}
{"id":2,"name":"name2"}
```

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*