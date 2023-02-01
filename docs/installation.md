# Installation

## Docker

Images are available on [GitHub container registry](https://github.com/ozontech/file.d/pkgs/container/file.d).

Mount config from /my-config.yaml and start `file.d` with this config: <br>
`docker run -v /my-config.yaml:/my-config.yaml ghcr.io/ozontech/file.d:v0.8.9 /file.d/file.d --config /my-config.yaml`

## Precompiled binaries

Precompiled binaries for released versions are available in the
[GitHub releases](https://github.com/ozontech/file.d/releases).

## From source

You can install a binary release using Go:

```shell
go install -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=v0.8.9" github.com/ozontech/file.d/cmd/file.d@v0.8.9
```
