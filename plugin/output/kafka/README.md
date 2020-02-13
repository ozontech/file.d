# Kafka output
Sends event batches to kafka brokers using `sarama` lib.

### Config params
**`brokers`** *`[]string`* *`required`* 

List of kafka brokers to write to.

<br>

**`default_topic`** *`string`* *`required`* 

Default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.

<br>

**`use_topic_field`** *`bool`* *`default=false`* 

If set plugin will use topic name from the event field.

<br>

**`topic_field`** *`string`* *`default=topic`* 

Which event field to use as topic name. Works only if `should_use_topic_field` is set.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How much workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

Maximum quantity of events to pack into one batch.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't full.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*