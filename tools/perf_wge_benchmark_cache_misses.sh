#!/bin/bash
if [[ ! -d perf ]]; then
  mkdir perf
fi
LD_LIBRARY_PATH=LD_LIBRARY_PATH=/usr/local/lib64 perf stat \
  -e L1-dcache-loads,L1-dcache-load-misses,L1-icache-load-misses \
  -e cache-references,cache-misses \
  -e cycles,instructions,branches,branch-misses \
  -- build/release-with-debug-info/benchmarks/wge/wge_benchmark $@