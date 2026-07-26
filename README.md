# Lock-free SPSC Ring Buffer (C++20)

Minimal single-producer / single-consumer ring buffer with a correctness test and a throughput / paced-latency benchmark.

## Approach

- **SPSC only** — one thread `try_push`, one other thread `try_pop` (no locks, no CAS).
- **Two build variants** (same API), selected with `SPSC_BASELINE`:
  - **Baseline** — masked indices, one slot left empty, every push/pop loads the remote index.
  - **Optimized** (default) — monotonic counters (full `Capacity` usable) + cached remote indices, refreshed only on full/empty.
- **Cache-line separation** — producer and consumer state on different lines.
- **Acquire/release atomics** — payload is visible before the index update is observed.
- Bench/test pin producer → CPU 0 and consumer → CPU 2 when the OS supports it (**Windows** and **Linux** via `cpu_affinity.hpp`). On other platforms (e.g. macOS) pinning is a no-op.

## API

```cpp
#include "spsc_ring.hpp"

SpscRing<int, 1024> q;  // Capacity must be a power of two

bool ok = q.try_push(42);   // producer only; false if full
int x;
bool got = q.try_pop(x);    // consumer only; false if empty

q.size_approx();            // racy occupancy (diagnostics)
q.capacity();               // usable slots
q.variant();                // "baseline" or "optimized"
```

| Method | Thread | Notes |
|--------|--------|--------|
| `try_push(U&&)` | producer only | Constructs in-place; `false` if full |
| `try_pop(T&)` | consumer only | Move-assigns out; `false` if empty |
| `capacity()` | any | Usable slots (`Capacity` optimized, `Capacity-1` baseline) |
| `variant()` | any | `"baseline"` or `"optimized"` |
| `size_approx()` | any | Approximate size; not for synchronization |

## Correctness test

Pushes `0 .. N-1` on one thread and asserts the consumer receives **all** values **in order** with an empty ring afterward.

```bat
cl /std:c++20 /O2 /EHsc test_correctness.cpp /Fe:test_correctness.exe
test_correctness.exe
test_correctness.exe 20000000

cl /std:c++20 /O2 /EHsc /DSPSC_BASELINE test_correctness.cpp /Fe:test_baseline.exe
test_baseline.exe
```

```bash
g++ -std=c++20 -O2 -pthread -o test_correctness test_correctness.cpp
./test_correctness
g++ -std=c++20 -O2 -pthread -DSPSC_BASELINE -o test_baseline test_correctness.cpp
./test_baseline
```

## Benchmark (reproducible baseline vs optimized)

Use the **same** iteration count for both builds:

```bat
cl /std:c++20 /O2 /EHsc /DNDEBUG bench.cpp /Fe:bench.exe
cl /std:c++20 /O2 /EHsc /DNDEBUG /DSPSC_BASELINE bench.cpp /Fe:bench_baseline.exe
bench_baseline.exe 50000000
bench.exe 50000000
```

```bash
g++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
g++ -std=c++20 -O3 -DNDEBUG -pthread -DSPSC_BASELINE -o bench_baseline bench.cpp
./bench_baseline 50000000
./bench 50000000
```

Each binary prints `Variant:` and `Affinity:` so you can confirm which build you ran. Latency keeps **one message in flight** (handoff, not backlog).

## Benchmark results

Windows, MSVC `/O2` (x64), **same** harness and `50000000` iterations, capacity template `1<<16`. Regenerate with the commands above.

| Metric | Baseline (`/DSPSC_BASELINE`) | Optimized (default) |
|--------|------------------------------|---------------------|
| Throughput | **206.04 Mmsg/s** | **326.34 Mmsg/s** (~1.58×) |
| Paced latency mean | 158.5 ns | 121.1 ns |
| Paced latency p50 | 100 ns | 100 ns |
| Paced latency p99 | 200 ns | 200 ns |

Correctness: both variants passed `test_correctness` / `test_baseline` for 5M in-order sequence numbers.

## Files

| File | Purpose |
|------|---------|
| `spsc_ring.hpp` | `SpscRing` — optimized by default; `SPSC_BASELINE` for the original |
| `cpu_affinity.hpp` | `pin_to_cpu` / `cpu_relax` (Windows + Linux) |
| `test_correctness.cpp` | In-order, no-drop sequence test |
| `bench.cpp` | Throughput + paced latency |
| `README.md` | Approach, API, how to reproduce |
