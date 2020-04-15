# Date convert plugin
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=time`* 

The event field name which contains date information.

<br>

**`source_formats`** *`[]string`* *`default=rfc3339nano,rfc3339`* 

List of date formats to parse a field. Available list items should be one of `ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano`.

<br>

**`target_format`** *`string`* *`default=timestamp`* 

Date format to convert to.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*