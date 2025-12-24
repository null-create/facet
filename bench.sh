#!/usr/bin/bash

set -e

if ! make bench-all; then
  echo "bench tests failed"
  exit 1
fi

echo "capturing results..."

touch bench_results.txt
go tool pprof -top cpu.prof >> bench_results.txt
go tool pprof -top mem.prof >> bench_results.txt
go tool pprof -top block.prof >> bench_results.txt
go tool pprof -top mutex.prof >> bench_results.txt

echo "done"