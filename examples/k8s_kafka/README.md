# K8s to Kafka

This folder contains our k8s daemonset configurations, where the input plugin is
[k8s](/plugin/input/k8s)
and the output plugin is
[kafka](/plugin/output/kafka).

If necessary, you can redirect our logs to other topics.
For example, we want to store audit logs in another topic that
another file.d will process according to the desired logic.
In this config we are redirect logs from the seqdb service to the topic `obs-seq-db-logs`.

## Configs

- [filed_obs](./filed_obs.yaml) - consumes k8s pod's logs, applies K8s labels and stores them in Kafka

See these examples as we continue to process logs after K8s:
[kafka_elasticsearch](/examples/kafka_elasticsearch)
[kafka_clickhouse](/examples/kafka_clickhouse)
