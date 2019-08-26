.PHONY: test
test:
	go test ./filed/ -v -count 1
	go test ./plugin/... -v -count 1

.PHONY: bench-file
bench-file:
	go test -bench LightJsonReadPar ./plugin/inputfile -v -count 1 -run -benchmem -benchtime 1x

.PHONY: profile-file
profile-file:
	go test -bench LightJsonReadPar ./plugin/inputfile -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof

.PHONY: build
build:
	GOOS=linux GOARCH=amd64 go build -o file-d ./cmd/filed.go

.PHONY: push-image
push-image: build
	@if [[ "${VERSION}" == "" ]]; then \
		echo "Usage push-image VERSION=vX.X.X"; exit 1; \
	fi

	docker build -t gitlab-registry.ozon.ru/sre/filed:${VERSION} .
	docker push gitlab-registry.ozon.ru/sre/filed:${VERSION}

