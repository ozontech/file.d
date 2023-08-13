ARG APP_IMAGE=ubuntu:latest

# Build
FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS build

ARG VERSION
ARG BUILD_TIME

WORKDIR /file.d

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ENV CGO_ENABLED 0
ENV GOOS linux
ENV GOARCH amd64

RUN go build -trimpath \
    -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=${VERSION} \
    -X github.com/ozontech/file.d/buildinfo.BuildTime=${BUILD_TIME}" \
    -o file.d ./cmd/file.d

# Deploy
FROM $APP_IMAGE

RUN apt update
RUN apt install systemd strace tcpdump traceroute telnet iotop curl jq iputils-ping htop -y

WORKDIR /file.d

COPY --from=build /file.d/file.d /file.d/file.d

CMD [ "./file.d" ]
