# Parse elasticsearch plugin
Plugin parses HTTP input using Elasticsearch /_bulk API format: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
It converts sources defining by create/index actions to the events. Update/delete actions are ignored.

> No config params
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*