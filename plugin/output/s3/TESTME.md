If you need test this pipeline locally do next steps:

#### 1. Create minio server with docker-compose up -d

Example docker-compose.yml:
```dockerfile
version: '3'
services:
  s3:
    image: minio/minio
    ports:
      - "19001:19001"
    volumes:
      - ./storage/minio:/data
    environment:
      MINIO_ACCESS_KEY: minio_access_key
      MINIO_SECRET_KEY: minio_secret_key
    command: server --address 0.0.0.0:19001 /data
```

#### 2. Create buckets on minio server

Creation of s3 buckets example:
```go
package main

import "github.com/minio/minio-go"

func main() {
	client, err := minio.New("0.0.0.0:19001", "minio_access_key", "minio_secret_key", false)
	if err != nil {
		panic(err)
	}

	err = client.MakeBucket("my-test-bucket1", "us-east-1")
	if err != nil {
		panic(err)  
	}
}
```

#### 3. Run file.d with new buckets
`sudo go run ./cmd/file.d --config=testconfig.yaml`

Example config:
```yaml
pipelines:
  s3:
    settings:
      capacity: 128
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
      endpoint: "0.0.0.0:19001"
      access_key: "minio_access_key"
      secret_key: "minio_secret_key"
      bucket: "my-test-bucket1"
      # If field "bucket_name" appears in event it'll be sent to bucket with name of this field value. 
      bucket_field_event: "bucket_name"
      
      # To test writing in buckets on other minio servers you need:
      # Run second minio server like on step 1 with other port, for example 20000.
      # Create bucket on this server like on step 2, let it be "other_bucket".
      # Send messages where field in 'bucket_name' (value of 'bucket_field_event') == 'other_bucket'.
      multi_buckets:
        - endpoint: "0.0.0.0:20000"
          access_key: "minio_access_key_1"
          secret_key: "minio_secret_key_1"
          bucket: "other_bucket"

```

#### 4. Send events!
```shell
# this goes to default bucket "my-test-bucket1".
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"message": "memento-mori", "kind": "normal"}
'

# this goes to bucket with name "dynamic_bucket_1" if it exits or to "my-test-bucket1" otherwise. 
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"message": "memento-mori", "kind": "normal", "bucket_name": "dynamic_bucket_1"}
'

# this goes to multi_bucket "other_bucket"
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"message": "memento-mori", "kind": "normal", "bucket_name": "other_bucket"}
'
```
