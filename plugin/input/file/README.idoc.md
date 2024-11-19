# File plugin
@introduction

**Reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    input:
        type: file
        paths:
          include:
            - '/var/lib/docker/containers/**/*-json.log'
          exclude:
            - '/var/lib/docker/containers/19aa5027343f4*/*-json.log'
        offsets_file: /data/offsets.yaml
        persistence_mode: async
```

### Config params
@config-params|description

### Meta params
**`filename`** 

**`symlink`** 

**`inode`** 

**`offset`** 
