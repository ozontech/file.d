# Join Template plugin
Alias to "join" plugin with predefined `start` and `continue` parameters.

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Enable explicit checks without regular expressions (use `fast_check` flag) or
> consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join_template
      template: go_panic
      field: log
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```

### Config params
**`field`** *`cfg.FieldSelector`* *`default=log`* *`required`* 

The event field which will be checked for joining with each other.

<br>

**`max_event_size`** *`int`* *`default=0`* 

Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.

<br>

**`template`** *`string`* 

The name of the template. Available templates: `go_panic`, `cs_exception`, `go_data_race`.

<br>

**`fast_check`** *`bool`* *`default=true`* 

Enable check without regular expressions.

<br>

**`templates`** *`[]TemplateConfig`* 

Configs of several templates. `TemplateConfig` params:
* **`name`** *`string`* *`required`*

	The name of the template. Available templates: `go_panic`, `cs_exception`, `go_data_race`.

* **`fast_check`** *`bool`*

	Enable check without regular expressions.


<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*