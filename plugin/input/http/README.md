# HTTP plugin
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HTTP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

### Config params
**`address`** *`string`* *`default=:9200`* 

An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`

<br>

**`emulate_mode`** *`string`* *`default=no`* *`options=no|elasticsearch`* 

Which protocol to emulate.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*