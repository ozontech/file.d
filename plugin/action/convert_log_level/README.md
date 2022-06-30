# Convert log level plugin
It converts the log level field according RFC-5424.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=level`* 

The name of the event field to convert.
The value of the field will be converted to lower case and trimmed before parsing.

<br>

**`style`** *`string`* *`default=number`* *`options=number|string`* 

Style format to convert. Must be one of number or string.
Available RFC-5424 levels:
<ul>
<li>0: emergency</li>
<li>1: alert </li>
<li>2: critical </li>
<li>3: error </li>
<li>4: warning </li>
<li>5: notice </li>
<li>6: informational </li>
</ul>

<br>

**`default_level`** *`string`* 

The default log level if the field cannot be parsed. If empty, no default level will be set.

<br>

**`remove_on_fail`** *`bool`* *`default=false`* 

Remove field if conversion fails.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*