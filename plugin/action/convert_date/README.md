# Date convert plugin
It converts field date/time data to different format.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=time`* 

The event field name which contains date information.

<br>

**`source_formats`** *`[]string`* *`default=rfc3339nano,rfc3339`* 

List of date formats to parse a field. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).

<br>

**`target_format`** *`string`* *`default=unixtime`* 

Date format to convert to. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).

<br>

**`remove_on_fail`** *`bool`* *`default=false`* 

Remove field if conversion fails.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*