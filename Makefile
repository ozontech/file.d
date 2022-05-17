VERSION ?= v0.5.5
UPSTREAM_BRANCH ?= origin/master

.PHONY: prepare
prepare:
	docker login

.PHONY: build
build: 
	echo "Building for amd64..."
	GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=${VERSION}" -v -o file.d ./cmd/file.d.go

.PHONY: build-for-current-system
build-for-current-system:
	echo "Building for current architecture..."
	go build -ldflags "-X main.version=${VERSION}" -v -o file.d ./cmd/file.d.go

.PHONY: deps
deps:
	go get -v github.com/vitkovskii/insane-doc@v0.0.1

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
	go test ./cmd/ -v -count 1

.PHONY: bench-file
bench-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: gen-doc
gen-doc:
	@go install github.com/vitkovskii/insane-doc@latest
	@~/go/bin/insane-doc

.PHONY: profile-file
profile-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof -mutexprofile mutex.pprof

.PHONY: push-version-linux-amd64
push-version-linux-amd64: build
	docker build -t ozonru/file.d:${VERSION}-linux-amd64 .
	docker push ozonru/file.d:${VERSION}-linux-amd64

.PHONY: push-latest-linux-amd64
push-latest-linux-amd64: build
	docker build -t ozonru/file.d:latest-linux-amd64 .
	docker push ozonru/file.d:latest-linux-amd64

.PHONY: push-images-version
push-images-version: prepare push-version-linux-amd64

.PHONY: push-images-latest
push-images-latest: prepare push-latest-linux-amd64

.PHONY: push-images-all
push-images-all: push-images-version push-images-latest

.PHONY: lint
lint:
	# installation: https://golangci-lint.run/usage/install/#local-installation
	golangci-lint run --new-from-rev=${UPSTREAM_BRANCH}

.PHONY: mock
mock:
	@go get github.com/golang/mock/gomock
	@go install github.com/golang/mock/mockgen
	@~/go/bin/mockgen -source=plugin/output/s3/s3.go -destination=plugin/output/s3/mock/s3.go
	@~/go/bin/mockgen -source=plugin/output/postgres/postgres.go -destination=plugin/output/postgres/mock/postgres.go
