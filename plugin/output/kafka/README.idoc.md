# Kafka output
@introduction

### Config params
@config-params|description

#### SASL config
**`enabled`** *`bool`* *`default=false`*

If set, the plugin will use SASL authentications mechanism.

<br>

**`mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512`*

SASL mechanism to use.

<br>

**`username`** *`string`* *`default=user`*

SASL username.

<br>

**`password`** *`string`* *`default=password`*

SASL password.

<br>

#### TLS config
**`enabled`** *`bool`* *`default=false`*

If set, the plugin will use SSL/TLS connections method.

<br>

**`skip_verify`** *`bool`* *`default=false`*

If set, the plugin will skip verification.

<br>

**`ca_cert`** *`string`* *`default=/file.d/certs`*

Path or content of a PEM-encoded CA file.

<br>
