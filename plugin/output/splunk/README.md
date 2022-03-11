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

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout the batch will be sent even if batch isn't completed.

<br>

**`event_key`** *`string`* 

This key will be extracted as "event" value

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*