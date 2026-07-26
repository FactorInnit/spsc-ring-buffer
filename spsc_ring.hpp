#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

// Minimal lock-free SPSC (single-producer / single-consumer) ring buffer.
// Capacity must be a power of two. One thread may push; one other thread may pop.
// One slot is left empty so full and empty are distinguishable without a size counter.
template <typename T, std::size_t Capacity>
class SpscRing {
  static_assert(Capacity >= 2, "Capacity must be at least 2");
  static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

  static constexpr std::size_t kMask = Capacity - 1;

#if defined(__cpp_lib_hardware_interference_size)
  static constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
  static constexpr std::size_t kCacheLine = 64;
#endif

 public:
  SpscRing() noexcept = default;

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  ~SpscRing() {
    // Drain remaining objects without requiring T to be default-constructible.
    std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t head = head_.load(std::memory_order_relaxed);
    while (tail != head) {
      slot(tail)->~T();
      tail = (tail + 1) & kMask;
    }
  }

  // Producer only. Returns false if the ring is full.
  template <typename U>
  bool try_push(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) & kMask;

    if (next == tail_.load(std::memory_order_acquire)) {
      return false;  // full
    }

    new (slot(head)) T(std::forward<U>(value));
    head_.store(next, std::memory_order_release);
    return true;
  }

  // Consumer only. Returns false if the ring is empty.
  bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T> ||
                                 std::is_nothrow_copy_assignable_v<T>) {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);

    if (tail == head_.load(std::memory_order_acquire)) {
      return false;  // empty
    }

    T* ptr = slot(tail);
    out = std::move(*ptr);
    ptr->~T();
    tail_.store((tail + 1) & kMask, std::memory_order_release);
    return true;
  }

  // Approximate occupancy (racy; diagnostics only).
  [[nodiscard]] std::size_t size_approx() const noexcept {
    const std::size_t head = head_.load(std::memory_order_acquire);
    const std::size_t tail = tail_.load(std::memory_order_acquire);
    return (head - tail) & kMask;
  }

  [[nodiscard]] static constexpr std::size_t capacity() noexcept {
    return Capacity - 1;
  }

 private:
  [[nodiscard]] T* slot(std::size_t index) noexcept {
    return reinterpret_cast<T*>(storage_) + index;
  }

  // Align head/tail on separate cache lines to avoid false sharing.
  alignas(kCacheLine) std::atomic<std::size_t> head_{0};
  alignas(kCacheLine) std::atomic<std::size_t> tail_{0};

  alignas(T) std::byte storage_[Capacity * sizeof(T)]{};
};
