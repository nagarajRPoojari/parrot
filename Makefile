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
	rm -rf $(BIN_DIR)

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

.PHONY: prof

prof:
	go tool pprof ./benchmark.test mem.out  