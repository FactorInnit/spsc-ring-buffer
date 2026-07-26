# Lock-free SPSC Ring Buffer (C++20)

Minimal single-producer / single-consumer ring buffer with a throughput and latency benchmark.

## Approach

- **SPSC only** — one thread `try_push`, one other thread `try_pop` (no locks, no CAS loops).
- **Power-of-two capacity** — wrap with a bitmask; one slot left empty so full ≠ empty.
- **Cache-line padded** `head_` / `tail_` — avoids false sharing between producer and consumer cores.
- **Acquire/release atomics** — data is published before the index update is visible to the other side.
- Placement-new into raw storage so slots are not default-constructed up front.

## API

```cpp
#include "spsc_ring.hpp"

SpscRing<int, 1024> q;  // Capacity must be a power of two (1023 usable)

// producer thread
bool ok = q.try_push(42);   // false if full

// consumer thread
int x;
bool got = q.try_pop(x);    // false if empty

q.size_approx();            // racy occupancy (diagnostics)
q.capacity();               // usable slots = Capacity - 1
```

| Method | Thread | Notes |
|--------|--------|--------|
| `try_push(U&&)` | producer only | Constructs in-place; returns `false` if full |
| `try_pop(T&)` | consumer only | Move-assigns out; returns `false` if empty |
| `capacity()` | any | Compile-time usable capacity |
| `size_approx()` | any | Approximate size; not for synchronization |

## Build & run

**MSVC** (x64 Native Tools / Developer Command Prompt):

```bat
cl /std:c++20 /O2 /EHsc /DNDEBUG bench.cpp /Fe:bench.exe
bench.exe
bench.exe 50000000
```

**g++ / clang++:**

```bash
g++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
./bench
```

Optional argument: message count for throughput (default `20000000`). Latency uses up to 2M samples.

## Backtest / benchmark results

Measured on Windows with MSVC `/O2`, 20M messages, ring usable capacity 65535:

| Metric | Result |
|--------|--------|
| Throughput | **156.95 Mmsg/s** |
| Elapsed (throughput) | 0.127 s |
| Messages consumed | 20,000,000 |
| Latency samples | 2,000,000 |
| Latency mean | 1,401,545.8 ns |
| Latency p50 | 1,664,800 ns |
| Latency p90 | 2,308,500 ns |
| Latency p99 | 4,285,400 ns |
| Latency p99.9 | 4,842,800 ns |
| Latency min / max | 100 ns / 4,926,300 ns |

**How to read latency:** the harness is unpaced (producer runs flat-out), so mean/percentiles include **queue backlog**, not only handoff delay. The **min (~100 ns)** is closer to an empty-queue transfer; millisecond-scale p50 reflects messages waiting in a full ring.

## Files

| File | Purpose |
|------|---------|
| `spsc_ring.hpp` | Header-only `SpscRing<T, Capacity>` |
| `bench.cpp` | Throughput + one-way latency harness |
| `README.md` | Approach, API, results |
