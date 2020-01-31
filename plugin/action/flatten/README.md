# Flatten action plugin
Plugin extracts object keys and adds them into root with some prefix.

**Example:**
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```

Will transform `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

## Config params
### field

`string`   

To be filled

### prefix

`string`   

To be filled


##
 *Generated using **insane-doc***