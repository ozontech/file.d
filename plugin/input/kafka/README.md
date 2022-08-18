# Kafka plugin
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.

### Config params
**`brokers`** *`[]string`* *`required`* 

The name of kafka brokers to read from.

<br>

**`topics`** *`[]string`* *`required`* 

The list of kafka topics to read from.

<br>

**`consumer_group`** *`string`* *`default=file-d`* 

The name of consumer group to use.

<br>

**`channel_buffer_size`** *`int`* *`default=256`* 

The number of unprocessed messages in the buffer that are loaded in the background from kafka.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*