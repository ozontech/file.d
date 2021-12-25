# s3 output
Sends events to s3 output of one or multiple buckets.
`bucket` is default bucket for events. Addition buckets can be described in `multi_buckets` section, example down here.
Field "bucket_field_in_event" is filed name, that will be searched in event.
If appears we try to send event to this bucket instead of described here.

**Example**
Standard example:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
      actions:
        - type: json_decode
          field: message
    output:
      type: s3
      file_plugin:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_in_event: "bucket_name"
```

Example with fan-out buckets:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
      actions:
        - type: json_decode
          field: message
    output:
      type: s3
      file_plugin:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      # bucket_field_in_event - event with such field will be sent to bucket with its value
      # if such exists: {"bucket_name": "secret", "message": 123} to bucket "secret".
      bucket_field_in_event: "bucket_name"
      # multi_buckets is optional, contains array of buckets.
      multi_buckets:
        - endpoint: "otherS3.fake_host.org:80"
          access_key: "access_key2"
          secret_key: "secret_key2"
          bucket: "bucket-logs-2"
        - endpoint: "yet_anotherS3.fake_host.ru:80"
          access_key: "access_key3"
          secret_key: "secret_key3"
          bucket: "bucket-logs-3"
```

### Config params
**`file_config`** *`file.Config`* 
Under the hood this plugin uses /plugin/output/file/ to collect logs

<br>

**`compression_type`** *`string`* *`default=zip`* *`options=zip`* 
Compression type

<br>

**`endpoint`** *`string`* *`required`* 
Endpoint address of default bucket.

<br>

**`access_key`** *`string`* *`required`* 
s3 access key.

<br>

**`secret_key`** *`string`* *`required`* 
s3 secret key.

<br>

**`bucket`** *`string`* *`required`* 
s3 default bucket.

<br>

**`multi_buckets`** *`[]singleBucketConfig`* 
MultiBuckets is additional buckets, which can also receive event.
Event must contain `bucket_name` field which value is s3 bucket name.
Events without `bucket_name` sends to DefaultBucket.

<br>

**`secure`** *`bool`* *`default=false`* 
s3 connection secure option.

<br>

**`bucket_field_in_event`** *`string`* *`default=bucket_name`* 
BucketFieldInEvent field change destination bucket of event to fields value.

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*