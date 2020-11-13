# Examples

## Kubernetes to kafka
The following config reads logs on k8s node, processes them and sends into kafka.
It assumes that k8s logs located in `/var/log/containers/` directory.
```yaml
pipelines:
  k8s_kafka_example:
    settings:
      antispam_threshold: 4000                  # ban files which spam logs
    input:
      type: file
      persistence_mode: async
      watching_dir: /var/log/containers/
      filename_pattern: "*"
      offsets_file: /data/k8s-offsets.yaml
    actions:
    - type: k8s                                 # add k8s meta information to event
      labels_whitelist: [app, jobid]            # add only this labels
      max_event_size: 131072
      metric_name: in                           # expose input metrics to prometheus
      metric_labels: [k8s_label_app, k8s_pod, k8s_container]
    - type: discard                             # discard some events 
      match_fields:
        k8s_namespace: /kube-system|ingress/
        k8s_container: /file-d/
      match_mode: or
    - type: join                                # join goland panics from stderr
      field: log
      start: '/^(panic:)|(http: panic serving)/'
      continue: '/(^\s*$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
      match_fields:
        stream: stderr
    - type: throttle                            # throttle pod logs if throughput is more than 3000/minute
      default_limit: 3000
      throttle_field: k8s_pod
      interval: 1m
      buckets: 60
    - type: keep_fields                         # keep only meaningful fields of event
      fields: [time, stream, log, k8s_namespace, k8s_pod, k8s_container, k8s_node, k8s_label_app, k8s_label_jobid]
    output:
      type: kafka
      brokers: [kafka-broker-0.svc.cluster.local, kafka-broker-1.svc.cluster.local, kafka-broker-2.svc.cluster.local]
      default_topic: k8s-logs
```

## Kafka to graylog
The following config reads logs from kafka, processes them and sends into gelf endpoint(graylog).
It assumes that logs are in docker json format.
```yaml
pipelines:
  kafka_gelf_example:
    input:
      type: kafka
      brokers: [kafka-broker-0.svc.local, kafka-broker-1.svc.local, kafka-broker-2.svc.local]
      topics: [k8s-logs]

    actions:
    - type: json_decode                             # unpack "log" field 
      field: log
      metric_name: input
      metric_labels: [k8s_label_app]                # expose input metrics to prometheus

    # normalize                                     # unify log format
    - type: rename
      log: message
      msg: message
      ts: time
      _ts: time
      systemd.unit: service
      syslog.identifier: service
      k8s_label_app: service

    output:
      type: gelf
      endpoint: "graylog.svc.cluster.local:12201"
      reconnect_interval: 1m
      default_short_message_value: "message isn't provided"
```

## What's next?
1. [Input](/plugin/input) plugins documentation
2. [Action](/plugin/action) plugins documentation
3. [Output](/plugin/output) plugins documentation