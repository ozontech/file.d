ARG APP_IMAGE=ubuntu:latest

# Build
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build

ARG VERSION
ARG BUILD_TIME
ARG TARGETARCH

WORKDIR /file.d

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ENV CGO_ENABLED 0
ENV GOOS linux
ENV GOARCH ${TARGETARCH:-amd64}

RUN go build -trimpath \
    -pgo default.pgo \
    -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=${VERSION}" \
    -o file.d ./cmd/file.d

# Deploy
FROM $APP_IMAGE

# Install systemd based on distribution
RUN if [ -f /etc/debian_version ]; then \
    apt-get update && apt-get install -y \
    systemd \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean; \
    elif [ -f /etc/redhat-release ]; then \
    dnf clean all || true \
    dnf update -y && dnf install -y \
    systemd \
    && dnf clean all; \
    fi

WORKDIR /file.d

COPY --from=build /file.d/file.d /file.d/file.d

CMD [ "./file.d" ]
