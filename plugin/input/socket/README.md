# Socket plugin
It reads events from socket network.

## Examples
TCP:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: tcp
      address: ':6666'
    ...
```
---
TLS:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: tcp
      address: ':6666'
      ca_cert: './cert.pem'
      private_key: './key.pem'
    ...
```
---
UDP:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: udp
      address: '[2001:db8::1]:1234'
    ...
```
---
Unix:
```yaml
pipelines:
  example_pipeline:
    ...
    input:
      type: socket
      network: unix
      address: '/tmp/filed.sock'
    ...
```

## Config params
**`network`** *`string`* *`default=tcp`* *`options=tcp|udp|unix`* 

Which network type to listen.

<br>

**`address`** *`string`* *`required`* 

An address to listen to.

For example:
- /tmp/filed.sock
- 1.2.3.4:9092
- :9092

<br>

**`ca_cert`** *`string`* 

CA certificate in PEM encoding. This can be a path or the content of the certificate.
> Works only if `network` is set to `tcp`.

<br>

**`private_key`** *`string`* 

CA private key in PEM encoding. This can be a path or the content of the key.
> Works only if `network` is set to `tcp`.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*