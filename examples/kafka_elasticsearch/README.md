# Kafka to ElasticSearch

This folder contains our k8s deployment configurations, where the input plugin is
[kafka](/plugin/input/kafka)
and the output plugin is
[elasticsearch](/plugin/output/elasticsearch).

## Configs

- [logd](./logd.yaml) - consumes kafka topics, applies RegExps to find token leaks and stores logs in Elasticsearch
