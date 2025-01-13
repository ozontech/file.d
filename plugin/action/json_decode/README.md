# JSON decode plugin
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

> ⚠ DEPRECATED. Use `decode` plugin with `decoder: json` instead.

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field to decode. Must be a string.

<br>

**`prefix`** *`string`* 

A prefix to add to decoded object keys.

<br>

**`log_json_parse_error_mode`** *`string`* *`default=off`* *`options=off|erronly|withnode`* 

Defines how to handle logging of json parse error.
*  `off` – do not log json parse errors
*  `erronly` – log only errors without any other data
*  `withnode` – log errors with json node represented as string

Defaults to `off`.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*