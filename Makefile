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


benchmark:
ifeq ($(filter read,$(MAKECMDGOALS)),read)
	@echo "Running read benchmark..."
	go test -bench=BenchmarkMemtable_Read -memprofile=mem.out -cpuprofile=cpu.out ./benchmark

else ifeq ($(filter write,$(MAKECMDGOALS)),write)
	@echo "Running write benchmark..."

ifeq ($(WAL),on)
	@echo "→ WAL enabled: running only WAL benchmark"
	go test -bench=BenchmarkMemtable_Write_With_WAL -memprofile=mem.out -cpuprofile=cpu.out ./benchmark

else ifeq ($(WAL),off)
	@echo "→ WAL disabled: running only non-WAL benchmark"
	go test -bench=BenchmarkMemtable_Write_Without_WAL -memprofile=mem.out -cpuprofile=cpu.out ./benchmark

else
	@echo "→ No WAL option provided: running both benchmarks"
	go test -bench=BenchmarkMemtable_Write_With_WAL -memprofile=mem.out -cpuprofile=cpu.out ./benchmark
	go test -bench=BenchmarkMemtable_Write_Without_WAL -memprofile=mem.out -cpuprofile=cpu.out ./benchmark
endif

else
	@echo "Usage: make benchmark read | write [WAL=on|off]"
endif

.PHONY: benchmark read write


mem_prof:
	go tool pprof ./benchmark.test mem.out  

go_prof:
	go tool pprof ./benchmark.test goroutine.prof

cpu_prof:
	go tool pprof ./benchmark.test cpu.out