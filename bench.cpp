// SPSC ring buffer benchmark: throughput + one-way latency.
//
// Build (MSVC):
//   cl /std:c++20 /O2 /EHsc /DNDEBUG bench.cpp /Fe:bench.exe
//
// Build (g++ / clang++):
//   g++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
//   clang++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
//
// Run:
//   ./bench
//   ./bench 50000000

#include "spsc_ring.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Msg {
  std::uint64_t seq;
  std::uint64_t tsc_ns;  // producer timestamp (steady_clock ns)
};

constexpr std::size_t kRingCapacity = 1u << 16;  // 65536 slots (65535 usable)

void pin_hint(const char* which) {
  // Soft hint only — OS may ignore. Helps reduce noise on noisy machines.
  (void)which;
#if defined(_WIN32)
  // No-op: affinity APIs are available but kept out of this minimal harness.
#else
  (void)which;
#endif
}

void bench_throughput(std::uint64_t iterations) {
  // Heap-allocate: ring storage is ~1MB and can overflow the default Windows stack.
  auto ring = std::make_unique<SpscRing<Msg, kRingCapacity>>();
  std::atomic<bool> start{false};
  std::atomic<std::uint64_t> consumed{0};

  std::thread consumer([&] {
    pin_hint("consumer");
    while (!start.load(std::memory_order_acquire)) {
      // wait
    }
    Msg m{};
    std::uint64_t n = 0;
    while (n < iterations) {
      if (ring->try_pop(m)) {
        ++n;
      }
    }
    consumed.store(n, std::memory_order_release);
  });

  std::thread producer([&] {
    pin_hint("producer");
    while (!start.load(std::memory_order_acquire)) {
      // wait
    }
    for (std::uint64_t i = 0; i < iterations; ++i) {
      Msg m{i, 0};
      while (!ring->try_push(m)) {
        // spin when full
      }
    }
  });

  const auto t0 = Clock::now();
  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  const auto t1 = Clock::now();

  const double secs =
      std::chrono::duration<double>(t1 - t0).count();
  const double mops = (static_cast<double>(iterations) / secs) / 1e6;

  std::cout << "=== Throughput (SPSC) ===\n"
            << "  messages : " << iterations << '\n'
            << "  elapsed  : " << std::fixed << std::setprecision(3) << secs
            << " s\n"
            << "  rate     : " << std::setprecision(2) << mops << " Mmsg/s\n"
            << "  consumed : " << consumed.load() << '\n';
}

void bench_latency(std::uint64_t iterations) {
  // One-way latency: producer stamps each message; consumer records delta.
  // Warm up first so cold-cache / thread start noise is excluded.
  auto ring = std::make_unique<SpscRing<Msg, kRingCapacity>>();
  std::atomic<bool> start{false};
  std::vector<std::uint64_t> samples;
  samples.reserve(static_cast<std::size_t>(iterations));

  const std::uint64_t warmup = std::min<std::uint64_t>(iterations / 10, 100000);

  std::thread consumer([&] {
    pin_hint("consumer");
    while (!start.load(std::memory_order_acquire)) {
    }
    Msg m{};
    std::uint64_t n = 0;
    const std::uint64_t total = warmup + iterations;
    while (n < total) {
      if (!ring->try_pop(m)) {
        continue;
      }
      const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           Clock::now().time_since_epoch())
                           .count();
      if (n >= warmup) {
        const auto delta = now - static_cast<std::int64_t>(m.tsc_ns);
        samples.push_back(static_cast<std::uint64_t>(delta < 0 ? 0 : delta));
      }
      ++n;
    }
  });

  std::thread producer([&] {
    pin_hint("producer");
    while (!start.load(std::memory_order_acquire)) {
    }
    const std::uint64_t total = warmup + iterations;
    for (std::uint64_t i = 0; i < total; ++i) {
      Msg m{};
      m.seq = i;
      m.tsc_ns = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              Clock::now().time_since_epoch())
              .count());
      while (!ring->try_push(m)) {
      }
    }
  });

  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();

  if (samples.empty()) {
    std::cout << "=== Latency ===\n  no samples collected\n";
    return;
  }

  std::sort(samples.begin(), samples.end());
  const auto percentile = [&](double p) -> std::uint64_t {
    const std::size_t idx = static_cast<std::size_t>(
        p * static_cast<double>(samples.size() - 1));
    return samples[idx];
  };

  const double mean =
      std::accumulate(samples.begin(), samples.end(), 0.0) /
      static_cast<double>(samples.size());

  std::cout << "=== One-way latency (producer stamp -> consumer) ===\n"
            << "  samples  : " << samples.size() << '\n'
            << "  mean     : " << std::fixed << std::setprecision(1) << mean
            << " ns\n"
            << "  p50      : " << percentile(0.50) << " ns\n"
            << "  p90      : " << percentile(0.90) << " ns\n"
            << "  p99      : " << percentile(0.99) << " ns\n"
            << "  p99.9    : " << percentile(0.999) << " ns\n"
            << "  min/max  : " << samples.front() << " / " << samples.back()
            << " ns\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t iterations = 20'000'000;
  if (argc > 1) {
    iterations = std::strtoull(argv[1], nullptr, 10);
    if (iterations < 1000) {
      iterations = 1000;
    }
  }

  std::cout << "SPSC ring capacity (usable): " << SpscRing<Msg, kRingCapacity>::capacity()
            << "\n"
            << "Iterations: " << iterations << "\n\n";

  bench_throughput(iterations);
  std::cout << '\n';
  // Latency uses fewer messages — clock_gettime / steady_clock overhead dominates.
  const std::uint64_t latency_iters =
      std::min<std::uint64_t>(iterations, 2'000'000);
  bench_latency(latency_iters);

  return 0;
}
