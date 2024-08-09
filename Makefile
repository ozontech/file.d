VERSION ?= $(shell git describe --abbrev=4 --dirty --always --tags)
TIME := $(shell date '+%Y-%m-%d_%H:%M:%S')
UPSTREAM_BRANCH ?= origin/master

.PHONY: build
build: 
	echo "Building for amd64..."
	GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-X github.com/ozontech/file.d/buildinfo.Version=${VERSION}" -o file.d ./cmd/file.d

.PHONY: cover
cover:
	go test -short -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
	rm coverage.out

.PHONY: test-short
test-short:
	go test ./fd/ -v -count 1 -short
	go test ./pipeline/ -v -count 1 -short
	go test ./plugin/... -v -count 1 -short

.PHONY: test
test:
	go test ./fd/ -v -count 1
	go test ./pipeline/ -v -count 1
	go test ./plugin/... -v -count 1

.PHONY: test-e2e
test-e2e:
	go test ./e2e -tags=e2e_new -v -count 1

.PHONY: test-e2e-docker-up
test-e2e-docker-up:
	for dc in $(shell find e2e -name 'docker-compose.yml') ; do \
		docker compose -f $$dc up -d ; \
	done

.PHONY: test-e2e-docker-down
test-e2e-docker-down:
	for dc in $(shell find e2e -name 'docker-compose.yml') ; do \
		docker compose -f $$dc down ; \
	done

.PHONY: bench-file
bench-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: gen-doc
gen-doc:
	go run github.com/vitkovskii/insane-doc@v0.0.3

.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

.PHONY: mock
mock:
	go run github.com/golang/mock/mockgen@v1.6.0 -source=plugin/output/s3/s3.go -destination=plugin/output/s3/mock/s3.go
	go run github.com/golang/mock/mockgen@v1.6.0 -source=plugin/output/postgres/postgres.go -destination=plugin/output/postgres/mock/postgres.go
	go run github.com/golang/mock/mockgen@v1.6.0 -source=plugin/output/clickhouse/clickhouse.go -destination=plugin/output/clickhouse/mock/clickhouse.go Clickhouse

.PHONY: generate
generate: gen-doc mock
	go generate ./...
