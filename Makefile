.PHONY: test
test:
	go test ./inputplugin/file -v -count 1

.PHONY: bench
bench:
	go test -bench LightJsonReadPar ./inputplugin/file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: profile
profile:
	go test -bench LightJsonReadPar ./inputplugin/file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof
