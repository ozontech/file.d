.PHONY: test
test:
	go test ./input/file -v -count 1

.PHONY: bench
bench:
	go test -bench LightJsonReadPar ./input/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: profile
profile:
	go test -bench LightJsonReadPar ./input/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof

.PHONY: build
build:
	GOOS=linux GOARCH=amd64 go build -o file-d ./cmd/filed.go
