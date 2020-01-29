# Kafka input plugin
Plugin reads events from listed kafka topics. It uses `sarama` lib.
It supports commitment mechanism, so it guaranties at least once delivery.

## Config params
### brokers

`string`  `required` 

Comma separated list of kafka brokers to read from.

### topics

`string`  `required` 

Comma separated list of kafka topics to read from.

### consumer_group

`string` `default=file-d`  

Name of consumer group to use.



 Generated using *insane-doc*