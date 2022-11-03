### Match modes
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
        match_mode: or
```

And if we process some logs:

```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"}         # pass
{"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"}         # pass
{"k8s_namespace": "map", "k8s_pod":"payment-api"}                  # pass
{"k8s_namespace": "payment", "k8s_pod":"map-api"}                  # pass
{"k8s_namespace": "tarifficator", "k8s_pod":"tarifficator-go-api"} # pass
{"k8s_namespace": "sre", "k8s_pod":"cpu-quotas-abcd-1234"}         # discard
```

<br>

#### Or
Or mode

<br>

#### AndPrefix
And prefix mode

<br>

#### Or
Or prefix mode

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*