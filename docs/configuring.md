# Configuring

You can specify several pipelines with plugins and their parameters in a yaml format.  
Examples can be found [here](./examples.md).

### Logging
Logging is configured with `LOG_LEVEL` environment variable ('info' by default).

Logging level can be changed in runtime with standard zap handler (go.uber.org/zap@v1.18.1/http_handler.go) exposed at `/log/level`.

### Overriding by environment variables
`file.d` can override config fields if you specify environment variables with `FILED_` prefix.  
The name of the env will be divided by underscores and the config will set or override the config field by the resulted path.  

As for now, overriding works only for fields in JSON objects, but array field overriding can be added easily, please submit an issue or pull request.  

For instance, in order to add Vault token `example_token` to the configuration, you should specify `FILED_VAULT_TOKEN=example_token`, and it will be added as:  
```yaml
vault:
  token: example_token
pipelines:
  pipeline_name: ...
```

### Vault support
Consider this config:
```yaml
vault:
  token: example_token
  address: http://127.0.0.1:8200
pipelines:
  k8s_kafka_example:
    input:
      type: file
      filename_pattern: vault(secret/prod/file_settings, filename_pattern)
    output:
      type: devnull
```

`file.d` supports getting secrets from Vault as soon as you specify Vault token and an address in a configuration.  
Then you can write any field-string in both arrays and dictionaries using syntax `vault(path/to/secret, key)`,  
and `file.d` tries to connect to Vault and get the secret from there.  
If you need to pass a literal string that begins with `vault(`, you should escape the value with a backslash: `\vault(path/to/secret, key)`.  
