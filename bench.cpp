// SPSC ring buffer benchmark: throughput + paced one-way latency.
//
// Optimized (default):
//   cl /std:c++20 /O2 /EHsc /DNDEBUG bench.cpp /Fe:bench.exe
//   g++ -std=c++20 -O3 -DNDEBUG -pthread -o bench bench.cpp
//
// Baseline (reproducible comparison):
//   cl /std:c++20 /O2 /EHsc /DNDEBUG /DSPSC_BASELINE bench.cpp /Fe:bench_baseline.exe
//   g++ -std=c++20 -O3 -DNDEBUG -pthread -DSPSC_BASELINE -o bench_baseline bench.cpp

#include "cpu_affinity.hpp"
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
  std::uint64_t tsc_ns;
};

constexpr std::size_t kRingCapacity = 1u << 16;

void bench_throughput(std::uint64_t iterations) {
  auto ring = std::make_unique<SpscRing<Msg, kRingCapacity>>();
  std::atomic<bool> start{false};
  std::atomic<std::uint64_t> consumed{0};

  std::thread consumer([&] {
    pin_to_cpu(2);
    while (!start.load(std::memory_order_acquire)) {
      cpu_relax();
    }
    Msg m{};
    std::uint64_t n = 0;
    while (n < iterations) {
      if (ring->try_pop(m)) {
        ++n;
      } else {
        cpu_relax();
      }
    }
    consumed.store(n, std::memory_order_release);
  });

  std::thread producer([&] {
    pin_to_cpu(0);
    while (!start.load(std::memory_order_acquire)) {
      cpu_relax();
    }
    for (std::uint64_t i = 0; i < iterations; ++i) {
      Msg m{i, 0};
      while (!ring->try_push(m)) {
        cpu_relax();
      }
    }
  });

  const auto t0 = Clock::now();
  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  const auto t1 = Clock::now();

  const double secs = std::chrono::duration<double>(t1 - t0).count();
  const double mops = (static_cast<double>(iterations) / secs) / 1e6;

  std::cout << "=== Throughput (SPSC) ===\n"
            << "  messages : " << iterations << '\n'
            << "  elapsed  : " << std::fixed << std::setprecision(3) << secs
            << " s\n"
            << "  rate     : " << std::setprecision(2) << mops << " Mmsg/s\n"
            << "  consumed : " << consumed.load() << '\n';
}

void bench_latency_paced(std::uint64_t iterations) {
  auto ring = std::make_unique<SpscRing<Msg, kRingCapacity>>();
  std::atomic<bool> start{false};
  std::vector<std::uint64_t> samples;
  samples.reserve(static_cast<std::size_t>(iterations));

  const std::uint64_t warmup = std::min<std::uint64_t>(iterations / 10, 100000);

  std::thread consumer([&] {
    pin_to_cpu(2);
    while (!start.load(std::memory_order_acquire)) {
      cpu_relax();
    }
    Msg m{};
    std::uint64_t n = 0;
    const std::uint64_t total = warmup + iterations;
    while (n < total) {
      if (!ring->try_pop(m)) {
        cpu_relax();
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
    pin_to_cpu(0);
    while (!start.load(std::memory_order_acquire)) {
      cpu_relax();
    }
    const std::uint64_t total = warmup + iterations;
    for (std::uint64_t i = 0; i < total; ++i) {
      while (ring->size_approx() > 0) {
        cpu_relax();
      }
      Msg m{};
      m.seq = i;
      m.tsc_ns = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              Clock::now().time_since_epoch())
              .count());
      while (!ring->try_push(m)) {
        cpu_relax();
      }
    }
  });

  start.store(true, std::memory_order_release);
  producer.join();
  consumer.join();

  if (samples.empty()) {
    std::cout << "=== Paced one-way latency ===\n  no samples collected\n";
    return;
  }

  std::sort(samples.begin(), samples.end());
  const auto percentile = [&](double p) -> std::uint64_t {
    const std::size_t idx =
        static_cast<std::size_t>(p * static_cast<double>(samples.size() - 1));
    return samples[idx];
  };

  const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) /
                      static_cast<double>(samples.size());

  std::cout << "=== Paced one-way latency (1 in-flight) ===\n"
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
  std::uint64_t iterations = 50'000'000;
  if (argc > 1) {
    iterations = std::strtoull(argv[1], nullptr, 10);
    if (iterations < 1000) {
      iterations = 1000;
    }
  }

  std::cout << "Variant: " << SpscRing<Msg, kRingCapacity>::variant() << "\n"
            << "Affinity: " << affinity_support() << "\n"
            << "SPSC ring capacity (usable): "
            << SpscRing<Msg, kRingCapacity>::capacity() << "\n"
            << "Iterations: " << iterations << "\n\n";

  bench_throughput(iterations);
  std::cout << '\n';
  const std::uint64_t latency_iters =
      std::min<std::uint64_t>(iterations, 1'000'000);
  bench_latency_paced(latency_iters);

  return 0;
}
