# Parse RE2 plugin
It parses string from the event field using re2 expression with named subgroups and merges the result with the event root.

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field to decode. Must be a string.

<br>

**`prefix`** *`string`* 

A prefix to add to decoded object keys.

<br>

**`is_logging`** *`bool`* *`default=false`* 

Field that includes logging (with sampling).

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*