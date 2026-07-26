#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

// Lock-free SPSC ring buffer (C++20).
// One thread may push; one other thread may pop.
//
// Define SPSC_BASELINE before including this header (or via -DSPSC_BASELINE /
// /DSPSC_BASELINE) to build the original empty-slot ring without cached indices.
// Default is the optimized monotonic-counter + cached-remote-index variant.

#if defined(SPSC_BASELINE)

// Baseline: masked indices, one slot left empty, no remote-index cache.
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
    std::size_t tail = tail_.load(std::memory_order_relaxed);
    const std::size_t head = head_.load(std::memory_order_relaxed);
    while (tail != head) {
      slot(tail)->~T();
      tail = (tail + 1) & kMask;
    }
  }

  template <typename U>
  bool try_push(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    const std::size_t next = (head + 1) & kMask;

    if (next == tail_.load(std::memory_order_acquire)) {
      return false;
    }

    new (slot(head)) T(std::forward<U>(value));
    head_.store(next, std::memory_order_release);
    return true;
  }

  bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T> ||
                                 std::is_nothrow_copy_assignable_v<T>) {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);

    if (tail == head_.load(std::memory_order_acquire)) {
      return false;
    }

    T* ptr = slot(tail);
    out = std::move(*ptr);
    ptr->~T();
    tail_.store((tail + 1) & kMask, std::memory_order_release);
    return true;
  }

  [[nodiscard]] std::size_t size_approx() const noexcept {
    const std::size_t head = head_.load(std::memory_order_acquire);
    const std::size_t tail = tail_.load(std::memory_order_acquire);
    return (head - tail) & kMask;
  }

  [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity - 1; }

  [[nodiscard]] static constexpr const char* variant() noexcept { return "baseline"; }

 private:
  [[nodiscard]] T* slot(std::size_t index) noexcept {
    return reinterpret_cast<T*>(storage_) + index;
  }

  alignas(kCacheLine) std::atomic<std::size_t> head_{0};
  alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
  alignas(kCacheLine) alignas(T) std::byte storage_[Capacity * sizeof(T)]{};
};

#else  // optimized (default)

// Optimized: monotonic counters (full Capacity usable) + cached remote indices.
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
    std::size_t tail = cons_.tail.load(std::memory_order_relaxed);
    const std::size_t head = prod_.head.load(std::memory_order_relaxed);
    while (tail != head) {
      slot(tail)->~T();
      ++tail;
    }
  }

  template <typename U>
  bool try_push(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
    const std::size_t head = prod_.head.load(std::memory_order_relaxed);

    if (head - prod_.cached_tail >= Capacity) [[unlikely]] {
      prod_.cached_tail = cons_.tail.load(std::memory_order_acquire);
      if (head - prod_.cached_tail >= Capacity) {
        return false;
      }
    }

    new (slot(head)) T(std::forward<U>(value));
    prod_.head.store(head + 1, std::memory_order_release);
    return true;
  }

  bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T> ||
                                 std::is_nothrow_copy_assignable_v<T>) {
    const std::size_t tail = cons_.tail.load(std::memory_order_relaxed);

    if (tail == cons_.cached_head) [[unlikely]] {
      cons_.cached_head = prod_.head.load(std::memory_order_acquire);
      if (tail == cons_.cached_head) {
        return false;
      }
    }

    T* ptr = slot(tail);
    out = std::move(*ptr);
    ptr->~T();
    cons_.tail.store(tail + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] std::size_t size_approx() const noexcept {
    const std::size_t head = prod_.head.load(std::memory_order_acquire);
    const std::size_t tail = cons_.tail.load(std::memory_order_acquire);
    return head - tail;
  }

  [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

  [[nodiscard]] static constexpr const char* variant() noexcept { return "optimized"; }

 private:
  [[nodiscard]] T* slot(std::size_t index) noexcept {
    return reinterpret_cast<T*>(storage_) + (index & kMask);
  }

  struct alignas(kCacheLine) ProducerState {
    std::atomic<std::size_t> head{0};
    std::size_t cached_tail{0};
  };

  struct alignas(kCacheLine) ConsumerState {
    std::atomic<std::size_t> tail{0};
    std::size_t cached_head{0};
  };

  ProducerState prod_{};
  ConsumerState cons_{};

  alignas(kCacheLine) alignas(T) std::byte storage_[Capacity * sizeof(T)]{};
};

#endif  // SPSC_BASELINE
