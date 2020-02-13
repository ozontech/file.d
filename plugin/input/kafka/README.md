# Kafka plugin
Reads events from multiple kafka topics using `sarama` library.
> It guaranties at least once delivery due to commitment mechanism.

### Config params
**`brokers`** *`[]string`* *`required`* 

List of kafka brokers to read from.

<br>

**`topics`** *`[]string`* *`required`* 

List of kafka topics to read from.

<br>

**`consumer_group`** *`string`* *`default=file-d`* 

Name of consumer group to use.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*