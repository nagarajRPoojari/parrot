# parrot

benchmark

go test -bench=BenchmarkMemtable_Intensive_Write_And_Read -memprofile=mem.out ./benchmark
go tool pprof ./benchmark.test mem.out  