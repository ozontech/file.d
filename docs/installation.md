# Installation

## Docker

Images are available
on [GitHub container registry](https://github.com/ozontech/file.d/pkgs/container/file.d/versions?filters%5Bversion_type%5D=tagged).

**Note**:
If you are using [journalctl](https://github.com/ozontech/file.d/tree/master/plugin/input/journalctl) input plugin, we
recommend choosing the ubuntu version that matches the host machine
version.
For example, if the host machine with which you want to collect logs using journald has a version of Ubuntu 18.04, you
should choose file.d with `-ubuntu18.04` suffix to avoid errors.

Mount config from /my-config.yaml and start `file.d` with this config: <br>
`docker run -v /my-config.yaml:/my-config.yaml ghcr.io/ozontech/file.d:v0.14.0 /file.d/file.d --config /my-config.yaml`

Do not forget to set [the actual tag](https://github.com/ozontech/file.d/pkgs/container/file.d) of the image.

## Helm-chart

[Helm-chart](/charts/filed/README.md) and examples for Minikube

## Precompiled binaries

Precompiled binaries for released versions are available in the
[GitHub releases](https://github.com/ozontech/file.d/releases).

## From source

You can install a binary release using Go:

```shell
go install -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=v0.14.0" github.com/ozontech/file.d/cmd/file.d@v0.14.0
```

Do not forget to set [the actual tag](https://github.com/ozontech/file.d/releases) of the file.d.
