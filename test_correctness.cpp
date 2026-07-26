// Correctness test: produce 0..N-1, consume in order, no drops/dupes.
//
//   cl /std:c++20 /O2 /EHsc test_correctness.cpp /Fe:test_correctness.exe
//   cl /std:c++20 /O2 /EHsc /DSPSC_BASELINE test_correctness.cpp /Fe:test_baseline.exe
//   g++ -std=c++20 -O2 -pthread -o test_correctness test_correctness.cpp

#include "cpu_affinity.hpp"
#include "spsc_ring.hpp"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

namespace {

constexpr std::size_t kRingCapacity = 1u << 10;  // 1024

bool run_sequence_test(std::uint64_t n) {
  auto ring = std::make_unique<SpscRing<std::uint64_t, kRingCapacity>>();
  std::atomic<bool> failed{false};
  std::atomic<std::uint64_t> received{0};

  std::thread consumer([&] {
    pin_to_cpu(2);
    std::uint64_t expect = 0;
    std::uint64_t value = 0;
    while (expect < n) {
      if (!ring->try_pop(value)) {
        cpu_relax();
        continue;
      }
      if (value != expect) {
        std::cerr << "order violation: got " << value << " expected " << expect
                  << '\n';
        failed.store(true, std::memory_order_release);
        return;
      }
      ++expect;
    }
    received.store(expect, std::memory_order_release);
  });

  std::thread producer([&] {
    pin_to_cpu(0);
    for (std::uint64_t i = 0; i < n; ++i) {
      while (!ring->try_push(i)) {
        if (failed.load(std::memory_order_acquire)) {
          return;
        }
        cpu_relax();
      }
    }
  });

  producer.join();
  consumer.join();

  if (failed.load(std::memory_order_acquire)) {
    return false;
  }
  if (received.load(std::memory_order_acquire) != n) {
    std::cerr << "drop/count mismatch: received "
              << received.load(std::memory_order_relaxed) << " expected " << n
              << '\n';
    return false;
  }
  if (ring->size_approx() != 0) {
    std::cerr << "ring not empty after drain: size_approx=" << ring->size_approx()
              << '\n';
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t n = 5'000'000;
  if (argc > 1) {
    n = std::strtoull(argv[1], nullptr, 10);
    if (n < 1) {
      n = 1;
    }
  }

  std::cout << "Variant: " << SpscRing<std::uint64_t, kRingCapacity>::variant()
            << "\n"
            << "Affinity: " << affinity_support() << "\n"
            << "Pushing " << n << " sequence numbers through capacity "
            << SpscRing<std::uint64_t, kRingCapacity>::capacity() << "...\n";

  if (!run_sequence_test(n)) {
    std::cerr << "FAIL\n";
    return 1;
  }

  std::cout << "OK: received all " << n << " values in order, no drops\n";
  return 0;
}
