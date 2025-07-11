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

.PHONY: benchmark

benchmark:
	go test -bench=BenchmarkMemtable_Intensive_Write_And_Read -memprofile=mem.out ./benchmark
	
prof:
	go tool pprof ./benchmark.test mem.out  