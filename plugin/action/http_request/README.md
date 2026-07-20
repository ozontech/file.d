# HTTP request plugin
Sends HTTP requests with event data as body. Writes response body to the configured response_field. Supports retry with exponential backoff, custom headers, URL templating.

## Example
```yaml
pipelines:
  - actions:
       	...
       - type: http_request
        address: "http://example.com/api/{{ .id | default "unknown" }}"
        method: GET
        content_type: "application/json"
        params:
          id: "field.id"
          user_id: "user"
        response_field: "http_response"
        retry: 3
        retention: 100ms
        timeout: 5s
    ...
```

# example of request to server:
# GET http://example.com/api/id_value?user_id=user


## Config params
**`params`** *`map[string]string`* 

Query parameters to add to the request.

<br>

**`method`** *`string`* *`default=POST`* *`options=POST|GET|PATCH`* 

HTTP method to use.

<br>

**`address`** *`string`* *`required`* 

URL address to send requests to.
Example: `http://localhost:8080/api`.com/v1/events`

<br>

**`timeout`** *`cfg.Duration`* *`default=5s`* 

Timeout for the HTTP request.

<br>

**`content_type`** *`string`* *`default=application/json`* 

Value of the Content-Type header.

<br>

**`response_field`** *`string`* 

Field name to store the HTTP response body.

<br>

**`headers`** *`map[string]string`* 

Custom headers to add to the HTTP request.

<br>

**`retry`** *`int`* *`default=10`* 

Number of retry attempts for failed HTTP requests.
Uses exponential backoff strategy between retries.
If all retries fail, the event is passed through without being sent.

<br>

**`retention`** *`cfg.Duration`* *`default=50ms`* 

Initial interval for exponential backoff between retries.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retry interval.
Each retry interval will be multiplied by this value.

<br>

**`success_codes`** *`[]int`* *`default=200`* 

List of HTTP status codes that are considered successful.

<br>

**`ca_cert`** *`string`* 

Path or content of a PEM-encoded CA file.

<br>

**`metric_prefix`** *`string`* 

Prefix added to metric names for better organization.
Useful when running multiple instances to avoid metric name collisions.
Leave empty for default metric naming.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*