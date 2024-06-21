# Meric plugin
Metric plugin.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: metric
	  name: errors_total
	  labels:
	  	- level

    ...
```


### Config params
**`name`** *`string`* *`default=total`* 

The metric name.

<br>

**`labels`** *`[]string`* 

Lists the event fields to add to the metric. Blank list means no labels.
Important note: labels metrics are not currently being cleared.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*