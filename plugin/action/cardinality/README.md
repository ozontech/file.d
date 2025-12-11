# Cardinality limit plugin
Limits the cardinality of fields on events or drops events.

**An example for discarding events with high cardinality field:**
```yaml
pipelines:
  example_pipeline:
    ...
    - type: cardinality
      limit: 10
      action: discard
      metric_prefix: service_zone
      key:
        - service
      fields:
        - zone
    ...
```

## Config params
**`key`** *`[]cfg.FieldSelector`* *`required`* 

Fields used to group events before calculating cardinality.
Events with the same key values are aggregated together.
Required for proper cardinality tracking per logical group.

<br>

**`fields`** *`[]cfg.FieldSelector`* *`required`* 

Target fields whose unique values are counted within each key group.
The plugin monitors how many distinct values these fields contain.
Required to define what constitutes high cardinality.

<br>

**`action`** *`string`* *`default=nothing`* *`options=discard|remove_fields|nothing`* 

Action to perform when cardinality limit is exceeded.
Determines whether to discard events, remove fields, or just monitor.
Choose based on whether you need to preserve other event data.

<br>

**`metric_prefix`** *`string`* 

Prefix added to metric names for better organization.
Useful when running multiple instances to avoid metric name collisions.
Leave empty for default metric naming.

<br>

**`metric_label_value`** *`string`* *`default=unknown`* 

Value assigned to the metric label when cardinality limit is exceeded.

<br>

**`limit`** *`int`* *`default=10000`* 

Maximum allowed number of unique values for monitored fields.
When exceeded within a key group, the configured action triggers.
Set based on expected diversity and system capacity.

<br>

**`ttl`** *`cfg.Duration`* *`default=1h`* 

Time-to-live for cardinality tracking cache entries.
Prevents unbounded memory growth by forgetting old unique values.
Should align with typical patterns of field value changes.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*