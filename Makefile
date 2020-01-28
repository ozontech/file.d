VERSION ?= v0.1.0

.PHONY: test
test:
	go test ./fd/ -v -count 1
	go test ./pipeline/ -v -count 1
	go test ./plugin/... -v -count 1

.PHONY: bench-file
bench-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: profile-file
profile-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof -mutexprofile mutex.pprof

.PHONY: build
build:
	GOOS=linux GOARCH=amd64 go build -v -o file-d ./cmd/file_d.go

.PHONY: push-image
push-image: build
	docker build -t gitlab-registry.ozon.ru/sre/file-d:${VERSION} .
	docker push gitlab-registry.ozon.ru/sre/file-d:${VERSION}

