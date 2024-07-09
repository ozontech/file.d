# Remove fields plugin
It removes the list of the event fields and keeps others.
Nested fields supported: list subfield names separated with dot.
Example:
```
fields: ["a.b.c"]

# event before processing
{
  "a": {
    "b": {
      "c": 100,
      "d": "some"
    }
  }
}

# event after processing
{
  "a": {
    "b": {
      "d": "some" # "c" removed
    }
  }
}
```

If field name contains dots use backslash for escaping.
Example:
```
fields:
  - exception\.type

# event before processing
{
  "message": "Exception occurred",
  "exception.type": "SomeType"
}

# event after processing
{
  "message": "Exception occurred" # "exception.type" removed
}
```

### Config params
**`fields`** *`[]string`* 

The list of the fields to remove.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*