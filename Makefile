.PHONY: test
test:
	go test ./plugins/input_file -v -count 1

.PHONY: bench
bench:
	go test -bench LightJsonReadPar ./plugins/input_file -v -count 1 -run  -benchmem -benchtime 1x -trace 1.trace
