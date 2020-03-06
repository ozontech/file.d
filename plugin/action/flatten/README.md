# Flatten plugin
It extracts the object keys and adds them into the root with some prefix. If the provided field isn't an object, an event will be skipped.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```
It transforms `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

Defines the field that should be flattened.

<br>

**`prefix`** *`string`* 

Which prefix to use for extracted fields.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*