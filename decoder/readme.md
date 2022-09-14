### Nginx decoder

Example of decoder for nginx logs with line joins

```yml

pipelines:
  example:
    actions:
      - type: join
        field: message
        start: '/^\d{1,7}#\d{1,7}\:.*/'
        continue: '/(^\w+)/'
      - type: convert_date
        source_formats: ['2006/01/02 15:04:05']
        target_format: 'rfc822'
    settings:
      decoder: 'nginx_error'
    input:
      type: file
      watching_dir: /dir
      offsets_file: /dir/offsets
      filename_pattern: "error.log"
      persistence_mode: async

    output:
      type: stdout
```
