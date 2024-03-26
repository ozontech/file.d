# Quick start

## Prerequisites 
1. docker
2. curl


## Steps
1. Put configuration file on your HDD:<br> 
`curl 'https://raw.githubusercontent.com/ozontech/file.d/master/testdata/config/welcome.yaml' > /tmp/welcome.yaml`

2. Put test data file on your HDD:<br>
`curl 'https://raw.githubusercontent.com/ozontech/file.d/master/testdata/json/welcome.json' > /tmp/welcome.json`

3. Run `file.d`:<br>
`docker run -v /tmp:/tmp ozonru/file.d:latest-linux-amd64 /file.d/file.d --config /tmp/welcome.yaml`

4. `file.d` will use data file as an input and your terminal as an output. So you'll see a welcome message.

## What's next?
1. Check out [more complex examples](/docs/examples.md) 
2. [Configure](/docs/configuring.md) your own pipeline 
3. [Helm-chart](/charts/filed/README.md) and examples for Minikube
