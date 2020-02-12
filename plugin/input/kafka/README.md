# Kafka input plugin
Plugin reads events from listed kafka topics. It uses `sarama` lib.
It supports commitment mechanism, so it guaranties at least once delivery.

## Config params
- **`brokers`** *`[]string`*   *`required`*  

List of kafka brokers to read from.
<br><br>

- **`topics`** *`[]string`*   *`required`*  

List of kafka topics to read from.
<br><br>

- **`consumer_group`** *`string`*  *`default=file-d`*   

Name of consumer group to use.
<br><br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*