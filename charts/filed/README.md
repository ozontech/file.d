# Helm Chart

Install [Helm](https://helm.sh/docs/intro/install/)

```shell
cd charts/filed

helm dependency update
```

## Examples for Minikube Minikube

Install [Minikube](https://minikube.sigs.k8s.io/docs/start/)

### Install collectors and storage for k8s-logs

```shell
helm upgrade --install k8s-logs . -f values.minikube.k8s-logs.yaml

kubectl create clusterrolebinding k8s-logs-filed --clusterrole=cluster-admin --serviceaccount=default:k8s-logs-filed

kubectl port-forward svc/k8s-logs-elasticsearch 9200:9200

curl -X GET "http://localhost:9200/k8s-logs/_search/?size=10" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "k8s_namespace": "ingress-nginx"
    }
  }
}'
```

### Collect logs from Web

```shell
helm upgrade --install web-logs . -f values.minikube.web-logs.yaml

curl --resolve "web-logs.local:80:$( minikube ip )" -H 'Content-Type: application/json' 'http://web-logs.local/?env=cli' -d \
'{"message": "Test event", "level": "info"}'

kubectl port-forward svc/web-logs-elasticsearch 9201:9200

curl -X GET "http://localhost:9201/web-logs-filed/_search/?size=10" -H 'Content-Type: application/json' -d '{
  "sort": [
    {"time": {"order":"desc"}}
  ]
}'
```

## TODO

- [ ] export metrics
- [ ] basics prometheus rules
- [ ] throttle with redis limiter_backend
