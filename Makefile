APP_NAME := lsm
SRC := ./...
BIN_DIR := bin

.PHONY: all build run clean test

all: build

build:
	go build -o $(BIN_DIR)/$(APP_NAME) .

run: build
	./$(BIN_DIR)/$(APP_NAME)

test:
	go test $(SRC) -v

race_test:
	go test $(SRC) --race -v

clean:
	rm -rf ./benchmark/test
	rm -rf ./benchmark/manifest
	rm -rf ./benchmark/*.log
	rm -rf $(BIN_DIR)
	rm ./benchmark.test
	rm mem.out

.PHONY: benchmark benchmark_read benchmark_write

benchmark:
ifeq ($(filter read,$(MAKECMDGOALS)),read)
	@echo "Running read benchmark..."
	go test -bench=BenchmarkMemtable_Intensive_Read -memprofile=mem.out ./benchmark
else ifeq ($(filter write,$(MAKECMDGOALS)),write)
	@echo "Running write benchmark..."
	go test -bench=BenchmarkMemtable_Intensive_Write -memprofile=mem.out ./benchmark
else
	@echo "Usage: make benchmark read | write"
endif

read:
write:

.PHONY: mem_prof go_prof

mem_prof:
	go tool pprof ./benchmark.test mem.out  

go_prof:
	go tool pprof ./benchmark.test goroutine.prof