ARG APP_IMAGE=ubuntu:latest

# Build
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build

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

WORKDIR /file.d

COPY --from=build /file.d/file.d /file.d/file.d

CMD [ "./file.d" ]
