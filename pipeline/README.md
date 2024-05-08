## Match modes
#### And
`match_mode: and` — matches fields with AND operator

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tarifficator] # use exact match
          k8s_pod: /^payment-api.*/              # use regexp match
        match_mode: and
```

result:
```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # discarded
{"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # discarded
{"k8s_namespace": "payment-tarifficator", "k8s_pod":"payment-api"} # won't be discarded
{"k8s_namespace": "tarifficator", "k8s_pod":"no-payment-api"}      # won't be discarded
```

<br>

#### Or
`match_mode: or` — matches fields with OR operator

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tarifficator] # use exact match
          k8s_pod: /^payment-api.*/              # use regexp match
        match_mode: or
```

result:
```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # discarded
{"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # discarded
{"k8s_namespace": "map", "k8s_pod":"payment-api"}                  # discarded
{"k8s_namespace": "payment", "k8s_pod":"map-api"}                  # discarded
{"k8s_namespace": "tarifficator", "k8s_pod":"tarifficator-go-api"} # discarded
{"k8s_namespace": "sre", "k8s_pod":"cpu-quotas-abcd-1234"}         # won't be discarded
```

<br>

#### AndPrefix
`match_mode: and_prefix` — matches fields with AND operator

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: payment # use prefix match
          k8s_pod: payment-api- # use prefix match
        match_mode: and_prefix
 ```

result:
```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"}    # discarded
{"k8s_namespace": "payment-2", "k8s_pod":"payment-api-abcd-1234"}  # discarded
{"k8s_namespace": "payment", "k8s_pod":"checkout"}                 # won't be discarded
{"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"}        # won't be discarded
{"k8s_namespace": "payment-abcd", "k8s_pod":"payment-api"}         # won't be discarded
```

<br>

#### OrPrefix
`match_mode: or_prefix` — matches fields with OR operator

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tarifficator] # use prefix match
          k8s_pod: /-api-.*/ # use regexp match
        match_mode: or_prefix
```

result:
```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"}    # discarded
{"k8s_namespace": "payment", "k8s_pod":"checkout"}                 # discarded
{"k8s_namespace": "map", "k8s_pod":"map-go-api-abcd-1234"}         # discarded
{"k8s_namespace": "map", "k8s_pod":"payment-api"}                  # won't be discarded
{"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"}        # discarded
{"k8s_namespace": "tariff", "k8s_pod":"tarifficator"}              # won't be discarded
```

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*