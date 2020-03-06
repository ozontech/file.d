# Quick start

## Prerequisites 
1. docker
2. curl


## Steps
1. Put configuration file on your HDD:<br> 
`curl 'https://raw.githubusercontent.com/ozonru/file.d/master/testdata/config/welcome.yaml' > /tmp/welcome.yaml`

2. Put test data file on your HDD:<br>
`curl 'https://raw.githubusercontent.com/ozonru/file.d/master/testdata/json/welcome.json' > /tmp/welcome.json`

3. Run `file.d`:<br>
`docker run -v /tmp:/tmp  docker.pkg.github.com/ozonru/file.d/file.d-linux-amd64:v0.1.2 /file.d/file.d --config /tmp/welcome.yaml`

4. `file.d` will use data file as an input and your terminal as an output. So you'll see a welcome message.

## What's next?
1. Check out [more complex examples](/docs/examples.md) 
2. [Configure](/docs/configuring.md) your own pipeline 