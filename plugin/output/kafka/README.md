# Kafka output
Plugin sends event batches to the kafka brokers. It uses `sarama` lib.

## Config params
### brokers

`string`  `required` 

Comma-separated list of kafka brokers to write to.

### default_topic

`string`  `required` 

Default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.

### use_topic_field

`bool` `default=false`  

If set plugin will use topic name from the event field.

### topic_field

`string` `default=topic`  

Which event field to use as topic name, if `should_use_topic_field` is set.

### workers_count

`cfg.Expression` `default=gomaxprocs*4`  

How much workers will be instantiated to send batches.

### batch_size

`cfg.Expression` `default=capacity/4`  

Maximum quantity of events to pack into one batch.

### batch_flush_timeout

`cfg.Duration`   

After this timeout batch will be sent even if batch isn't full.


##
 *Generated using **insane-doc***