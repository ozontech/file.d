# Date convert plugin
It converts field date/time data to different format.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=time`* 

The event field name which contains date information.

<br>

**`source_formats`** *`[]string`* *`default=rfc3339nano,rfc3339`* 

List of date formats to parse a field. Available list items should be one of `ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|timestamp|nginx_errorlog`.

<br>

**`target_format`** *`string`* *`default=timestamp`* 

Date format to convert to.

<br>

**`remove_on_fail`** *`bool`* *`default=false`* 

Remove field if conversion fails.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*