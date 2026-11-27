# HTTP request plugin
Makes HTTP requests with URL templating, query params, and custom headers. Writes response to event field. Supports retry with exponential backoff.

## Example
```yaml
pipelines:
  - example:
      actions:
        ...
        - type: http_request
          address: "http://example.com/api/{{ .id | default 'unknown' }}"
          method: GET
          content_type: "application/json"
          params:
            id: "field.id"
            user_id: "user"
          response_field: "http_response"
          retry: 3
          retention: 100ms
          timeout: 5s
```

### example of request to server:
```GET http://example.com/api/id_value?id=id_value&user_id=user```

All params are added to the query string, even if they are also used in the address template.
In this example `id` is sent both in the path and in the query string.


## Config params
**`params`** *`map[string]string`* 

Parameters to use in the address template and in the query string.
Each parameter is available in the `address` template as `{{ .name }}`
and is also added to the query string, even if it is already used in the template.
Parameter values are URL-escaped before being substituted into the address.

<br>

**`method`** *`string`* *`default=POST`* *`options=POST|GET|PATCH`* 

HTTP method to use.

<br>

**`address`** *`string`* *`required`* 

URL address to send requests to. Only `http` and `https` schemes are supported.
Example: `http://localhost:8080/api/v1/events`

<br>

**`timeout`** *`cfg.Duration`* *`default=5s`* 

Timeout for the HTTP request. Must be positive.

<br>

**`content_type`** *`string`* *`default=application/json`* 

Value of the Content-Type header.

<br>

**`response_field`** *`string`* 

Field name to store the HTTP response body.

<br>

**`force_http2`** *`bool`* *`default=false`* 

Force HTTP/2 for the request. Only supported for https addresses.
If the server doesn't support HTTP/2 or the connection can't be
established, the client falls back to HTTP/1.1.

<br>

**`headers`** *`map[string]string`* 

Custom headers to add to the HTTP request.

<br>

**`retry`** *`int`* *`default=10`* 

Number of retry attempts for failed HTTP requests.
A negative value means unlimited attempts, bounded by `max_elapsed_time`.
Uses exponential backoff strategy between retries.
If all retries fail, the event is passed through without being sent.

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Initial interval for exponential backoff between retries. Must be positive.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retry interval.
Each retry interval will be multiplied by this value. Must be positive.

<br>

**`max_elapsed_time`** *`cfg.Duration`* *`default=1m`* 

Maximum total time spent retrying a single request, including the time
spent executing requests. Zero disables the limit. Must be non-negative.

<br>

**`success_codes`** *`[]int`* *`default=200`* 

List of HTTP status codes that are considered successful.

<br>

**`tls`** *`*cfg.TLSConfig`* 

TLS configuration for the HTTP client.

`TLSConfig` params:
* **`ca_cert`** *`string`* - path or content of a PEM-encoded CA file
* **`client_cert`** *`string`* - path or content of a PEM-encoded client certificate file
* **`client_key`** *`string`* - path or content of a PEM-encoded client key file
* **`insecure`** *`bool`* - if set, the client will skip SSL/TLS verification

`client_cert` and `client_key` must be set together.

<br>

**`metric_prefix`** *`string`* 

Prefix added to metric names for better organization.
Useful when running multiple instances to avoid metric name collisions.
Leave empty for default metric naming.

<br>

**`fail_on_error`** *`bool`* *`default=false`* 

If set to false, errors will be logged instead of causing a fatal failure.
Useful for non-critical pipelines where you want to avoid stopping the entire pipeline on template errors.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*