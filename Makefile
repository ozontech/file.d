.PHONY: test
test:
	go test ./plugin/input_file -v -count 1

.PHONY: bench
bench:
	go test -bench LightJsonReadPar ./plugin/input_file -v -count 1 -run -benchmem -benchtime 1x

.PHONY: profile
profile:
	go test -bench LightJsonReadPar ./plugin/input_file -v -count 1 -run -benchmem -benchtime 1x -cpuprofile cpu.pprof -memprofile mem.pprof
