# JSON decode plugin
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field to decode. Must be a string.

<br>

**`prefix`** *`string`* 

A prefix to add to decoded object keys.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*