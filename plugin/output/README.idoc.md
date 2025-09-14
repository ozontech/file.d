# Output plugins

@global-contents-table-plugin-output|contents-table

## dead queue

Failed events from the main pipeline are redirected to a dead-letter queue (DLQ) to prevent data loss and enable recovery.

### Examples

#### Dead queue to the reserve elasticsearch

Consumes logs from a Kafka topic. Sends them to Elasticsearch (primary cluster). Fails over to a reserve ("dead-letter") Elasticsearch if the primary is unavailable.

```yaml
main_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs
  output:
    type: elasticsearch
    workers_count: 32
    endpoints:
      - http://elasticsearch-primary:9200
    # route to reserve elasticsearch
    deadqueue:
      endpoints:
        - http://elasticsearch-reserve:9200
      type: elasticsearch
```

#### Dead queue with second kafka topic and low priority consumer

Main Pipeline: Processes logs from Kafka → Elasticsearch. Failed events go to a dead-letter Kafka topic.

Dead-Queue Pipeline: Re-processes failed events from the DLQ topic with lower priority.

```yaml
main_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs
  output:
    type: elasticsearch
    workers_count: 32
    endpoints:
      - http://elasticsearch:9200
    # route to deadqueue pipeline
    deadqueue:
      brokers:
      - kafka:9092
      default_topic: logs-deadqueue
      type: kafka

deadqueue_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs-deadqueue
  output:
    type: elasticsearch
    workers_count: 1 # low priority
    fatal_on_failed_insert: false
    endpoints:
      - http://elasticsearch:9200
```