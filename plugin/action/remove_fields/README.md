# Remove fields plugin
It removes the list of the event fields and keeps others.

### Config params
**`fields`** *`[]string`* 

The list of the fields to remove.
Nested fields supported: list subfield names separated with dot.
Example:
```
fields: ["a.b.c"]
{"a":{"b":{"c": 100}}} -> {"a":{"b":{}}}
```

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*