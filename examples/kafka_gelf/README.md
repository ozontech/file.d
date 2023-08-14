# Kafka to ElasticSearch

This folder contains our k8s deployment configurations, where the input plugin is
[kafka](/plugin/input/kafka)
and the output plugin is
[gelf](/plugin/output/elasticsearch).

## Configs

- [kafka_gelf](./config.yaml) - consumes logs from Kafka,
  processes them and sends into gelf endpoint (e.g. graylog)
