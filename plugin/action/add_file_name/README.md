# File name adding plugin
It adds a field containing the file name to the event.
It is only applicable for input plugins k8s and file.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=file_name`* 

The event field to which put the file name. Must be a string.

Warn: it overrides fields if it contains non-object type on the path. For example:
if `field` is `info.level` and input
`{ "info": [{"userId":"12345"}] }`,
output will be: `{ "info": {"level": <level>} }`

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*