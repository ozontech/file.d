# Kafka to Clickhouse

This folder contains our k8s deployment configurations, where the input plugin is
[kafka](/plugin/input/kafka)
and the output plugin is
[clickhouse](/plugin/output/clickhouse).

## Configs

- [seqloghouse](./seqloghouse.yaml) - k8s deployment that consumes `obs-seq-db-logs` topic
  and stores logs in Clickhouse
