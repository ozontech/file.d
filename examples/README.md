# Examples

Check our file.d configuration files.
We use them all on our servers.
The most heavily loaded variation of file.d is daemonset with k8s plugin,
which serves a total of 2500+ servers, handles up to 3 million logs per second from all our clusters.

The second high-load variation of file.d is k8s deployment which reads from kafka and writes in elasticsearch.
We apply some regular expressions to logs to find secrets, phone numbers, tokens, cookies, and so on.
We do this on this service because it is easier to scale horizontally than daemonset.

**If we can not choose a config for ourselves, then welcome to
the [discussions](https://github.com/ozontech/file.d/discussions)!
We will help you to make a config.**

Checklist:

- [k8s_kafka](./k8s_kafka)
- [kafka_clickhouse](./kafka_clickhouse)
- [kafka_elasticsearch](./kafka_elasticsearch)
- [kafka_gelf](./kafka_gelf)
