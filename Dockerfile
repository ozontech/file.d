# Build
FROM golang:1.19-alpine AS build

RUN apk update
RUN apk add git

WORKDIR /file.d

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=$(git describe --abbrev=4 --dirty --always --tags) \
    -X github.com/ozontech/file.d/buildinfo.BuildTime=$(date '+%Y-%m-%d_%H:%M:%S')" \
    -o file.d ./cmd/file.d.go

# Deploy
FROM ubuntu:20.04

RUN apt update
RUN apt install systemd strace tcpdump traceroute telnet iotop curl jq iputils-ping htop -y

WORKDIR /file.d

COPY --from=build /file.d/file.d /file.d/file.d

CMD [ "./file.d" ]
