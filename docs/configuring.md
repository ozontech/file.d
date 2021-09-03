# Configuring

You can specify several pipelines with plugins and their parameters in a yaml format.  
Examples can be found [here](./examples.md).

### Overriding by environment variables
`file.d` can override a config if you specify environment variables with `FILED_` prefix.  
The name of the env will be divided by underscores and the config will set or override the config field by the resulted path.  

As for now, it lacks support of array fields overriding, and it works with dictionaries.  

For instance, on order to add Vault token to the configuration you can specify `FILED_VAULT_TOKEN=example_token`, and it will be added as:  
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
      filename_pattern: vault(/secret/prod/file_settings, filename_pattern)
    output:
      type: devnull
```

`file.d` supports getting secrets from Vault as soon as you specify Vault token and an address in a configuration.  
Then you can write any field-string in both arrays and dictionaries using syntax `vault(/path/to/secret, key)`,  
and `file.d` tries to connect to Vault and get the secret from there.  
