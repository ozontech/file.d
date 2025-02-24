# Set time plugin

It adds time field to the event.

### Config params
**`field`** *`string`* *`default=time`* *`required`* 

The event field to put the time.

<br>

**`format`** *`string`* *`default=rfc3339nano`* *`required`* 

Date format to parse a field. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).

<br>

**`override`** *`bool`* *`default=true`* 

Override field if exists.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*