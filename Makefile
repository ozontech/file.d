VERSION ?= 0.1.8

.PHONY: prepare
prepare:
	docker login

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
	insane-doc

.PHONY: profile-file
profile-file:
	go test -bench LightJsonReadPar ./plugin/input/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof -mutexprofile mutex.pprof

.PHONY: push-linux-amd64
push-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -v -o file.d ./cmd/file.d.go
	docker build -t ozonru/file.d:${VERSION}-linux-amd64 .
	docker push ozonru/file.d:${VERSION}-linux-amd64
	docker build -t ozonru/file.d:latest-linux-amd64 .
	docker push ozonru/file.d:latest-linux-amd64

.PHONY: push-images
push-images: prepare push-linux-amd64
