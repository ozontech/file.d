# HTTP plugin
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HTTP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

**Example:**
Emulating elastic through http:
```yaml
pipelines:
  example_k8s_pipeline:
    settings:
      capacity: 1024
    input:
      # define input type.
      type: http
      # pretend elastic search, emulate it's protocol.
      emulate_mode: "elasticsearch"
      # define http port.
      address: ":9200"
    actions:
      # parse elastic search query.
      - type: parse_es
      # decode elastic search json.
      - type: json_decode
        # field is required.
        field: message
    output:
      # Let's write to kafka example.
      type: kafka
      brokers: [kafka-local:9092, kafka-local:9091]
      default_topic: yourtopic-k8s-data
      use_topic_field: true
      topic_field: pipeline_kafka_topic

      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```bash
# run server.
# config.yaml should contains yaml config above.
go run ./cmd/file.d --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'
```

### Config params
**`address`** *`string`* *`default=:9200`* 

An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`

<br>

**`emulate_mode`** *`string`* *`default=no`* *`options=no|elasticsearch`* 

Which protocol to emulate.

<br>

**`ca_cert`** *`string`* 

CA certificate in PEM encoding. This can be a path or the content of the certificate.
If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.

<br>

**`private_key`** *`string`* 

CA private key in PEM encoding. This can be a path or the content of the key.
If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.

<br>

**`auth`** *`AuthConfig`* 

Auth config.
Disabled by default.
See AuthConfig for details.
You can use 'warn' log level for logging authorizations.

<br>

**`meta`** *`cfg.MetaTemplates`* 

Meta params

Add meta information to an event (look at Meta params)
Use [go-template](https://pkg.go.dev/text/template) syntax

Example: ```user_agent: '{{ index (index .request.Header "User-Agent") 0}}'```

<br>

**`header`** *`string`* *`default=Authorization`* 

Override default Authorization header

<br>

**`strategy`** *`string`* *`default=disabled`* *`options=disabled|basic|bearer`* 

AuthStrategy.Strategy describes strategy to use.

<br>

**`secrets`** *`map[string]string`* 

AuthStrategy.Secrets describes secrets in key-value format.
If the `strategy` is basic, then the key is the login, the value is the password.
If the `strategy` is bearer, then the key is the name, the value is the Bearer token.
Key uses in the http_input_total metric.

<br>


### Meta params
**`login`** 

**`remote_addr`**  *`net.IP`*

**`request`**  *`http.Request`*

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*