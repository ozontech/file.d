# splunk HTTP Event Collector output
It sends events to splunk.

### Config params
**`endpoint`** *`string`* *`required`* 

A full URI address of splunk HEC endpoint. Format: `http://127.0.0.1:8088/services/collector`.

<br>

**`token`** *`string`* *`required`* 

Token for an authentication for a HEC endpoint.

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


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*