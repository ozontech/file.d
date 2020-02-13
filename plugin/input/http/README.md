# HTTP plugin
Reads events from HTTP requests with body delimited by a new line.

Also it emulates some protocols to allow receive events from wide range of software which use HTTP to transmit data.
E.g. `file.d` may pretends to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it have read all the request body.
> It doesn't wait until events will be committed.

### Config params
**`address`** *`string`* *`default=:9200`* 

Address to listen to. Omit ip/host to listen for all network interfaces. E.g. `:88`

<br>

**`emulate_mode`** *`string`* *`default=no`* *`options=no|elasticsearch`* 

Which protocol to emulate.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*