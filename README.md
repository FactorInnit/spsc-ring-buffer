# Lock-free SPSC Ring Buffer (C++20)

Minimal single-producer / single-consumer ring buffer with a throughput and paced latency benchmark.

## Approach

- **SPSC only** — one thread `try_push`, one other thread `try_pop` (no locks, no CAS).
- **Power-of-two capacity** — slot index via bitmask; **all `Capacity` slots usable** (monotonic counters).
- **Cached remote indices** — producer caches `tail`, consumer caches `head`, refreshing only on full/empty. Cuts most cross-core atomic traffic (biggest speedup).
- **Cache-line separation** — producer state and consumer state on different lines (less false sharing).
- **Acquire/release atomics** — payload is visible before the index update is observed.
- Bench pins producer/consumer to separate cores and uses `pause` in spin waits; latency keeps **one message in flight** (no backlog distortion).

## API

```cpp
#include "spsc_ring.hpp"

SpscRing<int, 1024> q;  // Capacity must be a power of two (1024 usable)

// producer thread
bool ok = q.try_push(42);   // false if full

// consumer thread
int x;
bool got = q.try_pop(x);    // false if empty

q.size_approx();            // racy occupancy (diagnostics)
q.capacity();               // usable slots == Capacity
```

| Method | Thread | Notes |
|--------|--------|--------|
| `try_push(U&&)` | producer only | Constructs in-place; `false` if full |
| `try_pop(T&)` | consumer only | Move-assigns out; `false` if empty |
| `capacity()` | any | Compile-time usable capacity |
| `size_approx()` | any | Approximate size; not for synchronization |

## Build & run

**MSVC** (Developer Command Prompt):

```bat
cl /std:c++20 /O2 /EHsc /DNDEBUG bench.cpp /Fe:bench.exe
bench.exe
```

**g++ / clang++:**

```bash
g++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
./bench
```

Optional argument: throughput message count (default `50000000`). Latency uses up to 1M paced samples.

## Benchmark results

Windows, MSVC `/O2` (x64), ring capacity 65536.

| Metric | Baseline | Optimized |
|--------|----------|-----------|
| Throughput | **156.95 Mmsg/s** (20M msgs) | **239.43 Mmsg/s** (50M msgs) |
| Latency style | unpaced (includes backlog) | paced (1 in-flight) |
| Latency mean | ~1.4 ms | **135 ns** |
| Latency p50 | ~1.7 ms | **100 ns** |
| Latency p99 | ~4.3 ms | **200 ns** |
| Latency min | ~100 ns | **0–100 ns** (clock granularity) |

~**1.5×** throughput from cached indices + full-capacity ring + pinned threads. Paced latency is the fair handoff number; unpaced ms figures were mostly queue wait.

## Files

| File | Purpose |
|------|---------|
| `spsc_ring.hpp` | Header-only `SpscRing<T, Capacity>` |
| `bench.cpp` | Throughput + paced latency harness |
| `README.md` | Approach, API, results |
