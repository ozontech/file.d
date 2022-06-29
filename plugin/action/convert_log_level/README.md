# Convert log level plugin
It converts the log level field according RFC-5424.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=level`* 

The event field name which log level.

<br>

**`style`** *`string`* *`default=number`* *`options=number|string`* 

Date format to convert to.

<br>

**`default_level`** *`string`* 

Default log level if if cannot be parsed. Pass empty, to skip set default level.

<br>

**`remove_on_fail`** *`bool`* *`default=false`* 

Remove field if conversion fails.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*