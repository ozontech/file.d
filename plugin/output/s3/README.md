# s3 output
Sends events to s3 output of one or multiple buckets.
`bucket` is default bucket for events. Addition buckets can be described in `multi_buckets` section, example down here.
Field "bucket_field_event" is filed name, that will be searched in event.
If appears we try to send event to this bucket instead of described here.

> ⚠ Currently bucket names for bucket and multi_buckets can't intersect.

> ⚠ If dynamic bucket moved to config it can leave some not send data behind.
> To send this data to s3 move bucket dir from /var/log/dynamic_buckets/bucketName to /var/log/static_buckets/bucketName (/var/log is default path)
> and restart file.d

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
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
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
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      # bucket_field_event - event with such field will be sent to bucket with its value
      # if such exists: {"bucket_name": "secret", "message": 123} to bucket "secret".
      bucket_field_event: "bucket_name"
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

Under the hood this plugin uses /plugin/output/file/ to collect logs.

<br>

**`compression_type`** *`string`* *`default=zip`* *`options=zip`* 

Compressed files format.

<br>

**`endpoint`** *`string`* *`required`* 

Address of default bucket.

<br>

**`access_key`** *`string`* *`required`* 

S3 access key.

<br>

**`secret_key`** *`string`* *`required`* 

S3 secret key.

<br>

**`bucket`** *`string`* *`required`* 

Main S3 bucket.

<br>

**`multi_buckets`** *``json:"multi_buckets"`* 

Additional buckets, which can also receive event.
Event with bucket_name field sends to such s3 bucket.

<br>

**`secure`** *`bool`* *`default=false`* 

S3 connection secure option.

<br>

**`bucket_field_event`** *`string`* 

Change destination bucket of event.
Fallback to DefaultBucket if BucketEventField bucket doesn't exist.

<br>

**`dynamic_buckets_limit`** *`int`* *`default=32`* 

Regulates number of buckets that can be created dynamically.

<br>

**`upload_timeout`** *`cfg.Duration`* *`default=1m`* 

Sets upload timeout.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of upload. If File.d cannot upload for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not
**Experimental feature**

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to upload.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*