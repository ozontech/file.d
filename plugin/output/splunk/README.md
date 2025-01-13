# splunk HTTP Event Collector output
It sends events to splunk.

By default it only stores original event under the "event" key according to the Splunk output format.

If other fields are required it is possible to copy fields values from the original event to the other
fields relative to the output json. Copies are not allowed directly to the root of output event or
"event" field and any of its subfields.

For example, timestamps and service name can be copied to provide additional meta data to the Splunk:

```yaml
copy_fields:
  - from: ts
  	to: time
  - from: service
  	to: fields.service_name
```

Here the plugin will lookup for "ts" and "service" fields in the original event and if they are present
they will be copied to the output json starting on the same level as the "event" key. If the field is not
found in the original event plugin will not populate new field in output json.

In:

```json
{
  "ts":"1723651045",
  "service":"some-service",
  "message":"something happened"
}
```

Out:

```json
{
  "event": {
    "ts":"1723651045",
    "service":"some-service",
    "message":"something happened"
  },
  "time": "1723651045",
  "fields": {
    "service_name": "some-service"
  }
}
```

### Config params
**`endpoint`** *`string`* *`required`* 

A full URI address of splunk HEC endpoint. Format: `http://127.0.0.1:8088/services/collector`.

<br>

**`use_gzip`** *`bool`* *`default=false`* 

If set, the plugin will use gzip encoding.

<br>

**`gzip_compression_level`** *`string`* *`default=default`* *`options=default|no|best-speed|best-compression|huffman-only`* 

Gzip compression level. Used if `use_gzip=true`.

<br>

**`token`** *`string`* *`required`* 

Token for an authentication for a HEC endpoint.

<br>

**`keep_alive`** *`KeepAliveConfig`* 

Keep-alive config.

`KeepAliveConfig` params:
* `max_idle_conn_duration` - idle keep-alive connections are closed after this duration.
By default idle connections are closed after `10s`.
* `max_conn_duration` - keep-alive connections are closed after this duration.
If set to `0` - connection duration is unlimited.
By default connection duration is unlimited.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How many workers will be instantiated to send batches.

<br>

**`request_timeout`** *`cfg.Duration`* *`default=1s`* 

Client timeout when sends requests to HTTP Event Collector.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

A maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout the batch will be sent even if batch isn't completed.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not
**Experimental feature**

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to DB.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>

**`copy_fields`** *`[]CopyField`* 

List of field paths copy `from` field in original event `to` field in output json.
To fields paths are relative to output json - one level higher since original
event is stored under the "event" key. Supports nested fields in both from and to.
Supports copying whole original event, but does not allow to copy directly to the output root
or the "event" key with any of its subkeys.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*