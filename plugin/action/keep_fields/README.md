# Keep fields plugin
It keeps the list of the event fields and removes others.
Nested fields supported: list subfield names separated with dot.
Example:
```
fields: ["a.b.f1", "c"]
# event before processing
{
    "a":{
        "b":{
            "f1":1,
            "f2":2
        }
    },
    "c":0,
    "d":0
}

# event after processing
{
    "a":{
        "b":{
            "f1":1
        }
    },
    "c":0
}

```

NOTE: if `fields` param contains nested fields they will be removed.
For example `fields: ["a.b", "a"]` gives the same result as `fields: ["a"]`.
See `cfg.ParseNestedFields`.

### Config params
**`fields`** *`[]string`* 

The list of the fields to keep.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*