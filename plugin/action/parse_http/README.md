# File name adding plugin
It adds a field containing the source name to the event and extracts remote_ip and auth login.
It is only applicable for input plugin http.
You need add action add_file_name before it.

**Example:**
Service for receiving events from a static page:
```yaml
pipelines:
  example_http_pipeline:
    input:
      # define input type.
      type: http
      # define http port.
      address: ":9400"
      auth:
        strategy: basic
        secrets:
          frontend: 398fc645-e660-45f4-96bb-53b7a2b120e4
      cors:
          allowed_headers:
            - DNT
            - X-CustomHeader
            - Keep-Alive
            - User-Agent
            - X-Requested-With
            - If-Modified-Since
            - Cache-Control
            - Content-Type
            - Authorization
          allowed_origins:
            - http://localhost:8090
    actions:
    - type: add_file_name
      field: source_name
      # parse http info
    - type: parse_http
      field: source_name
      allowed_params:
        - env
    - type: convert_log_level
      field: level
      style: number
      default_level: info
      remove_on_fail: true
    - type: mask
      metric_name: errors_total
      metric_skip_status: true
      metric_labels:
        - login
        - level
    - type: remove_fields
      fields:
        - source_name
    output:
      type: stdout
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
curl "127.0.0.1:9400/?env=cli" -H 'Content-Type: application/json' -H 'Authorization: Basic ZnJvbnRlbmQ6Mzk4ZmM2NDUtZTY2MC00NWY0LTk2YmItNTNiN2EyYjEyMGU0' -d \
'{"message": "Test event", "level": "info"}'

# run nginx with static page
docker run -p 8090:80 -v `pwd`/plugin/action/parse_http:/usr/share/nginx/html -it --rm --name my-static-html-server nginx:alpine

# open http://localhost:8090 and click "Send Log Request" button

# check metric
curl localhost:9000/metrics | grep "file_d_pipeline_example_http_pipeline_errors_total_events_count_total"
```

### Config params
**`field`** *`cfg.FieldSelector`* *`default=source_name`* 

The event field to which put the source name. Must be a string.


<br>

**`allowed_params`** *`[]string`* 

List of the allowed GET params.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*