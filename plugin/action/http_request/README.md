# HTTP request plugin
Sends HTTP requests with event data as body. Writes response body to the configured response_field. Supports retry with exponential backoff, custom headers, URL templating.

## Example
```yaml
pipelines:
  - name: http_pipeline
    actions:
    	...
      - type: http_request
        address: "http://example.com/api/{tenant_id}"
        method: POST
        content_type: "application/json"
        params:
          tenant_id: "field.tenant"
          user_id: "user"
        response_field: "http_response"
        retry: 3
        retention: 100ms
        timeout: 5s
    ...
```


## Config params
**`params`** 
<br>

**`method`** 
<br>

**`address`** 
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

**`success_codes`** *`[]int`* 

List of HTTP status codes that are considered successful.

<br>

**`metric_prefix`** *`string`* 

Prefix added to metric names for better organization.
Useful when running multiple instances to avoid metric name collisions.
Leave empty for default metric naming.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*