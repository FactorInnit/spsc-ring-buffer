#pragma once

#include <thread>

#if defined(_WIN32)
#include <windows.h>
#if defined(_MSC_VER)
#include <intrin.h>
#endif
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#if defined(_MSC_VER)
#include <intrin.h>
#endif
#endif

inline void cpu_relax() noexcept {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
  _mm_pause();
#elif defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#else
  std::this_thread::yield();
#endif
}

// Pin the calling thread to a logical CPU.
// Supported: Windows (SetThreadAffinityMask), Linux (pthread_setaffinity_np).
// Elsewhere (e.g. macOS): no-op — OS may migrate the thread freely.
inline bool pin_to_cpu(unsigned cpu) {
#if defined(_WIN32)
  const DWORD_PTR mask = (sizeof(DWORD_PTR) >= 8)
                             ? (static_cast<DWORD_PTR>(1ull) << cpu)
                             : (static_cast<DWORD_PTR>(1u) << cpu);
  return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#elif defined(__linux__)
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<int>(cpu), &set);
  return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
  (void)cpu;
  return false;
#endif
}

inline constexpr const char* affinity_support() noexcept {
#if defined(_WIN32)
  return "windows";
#elif defined(__linux__)
  return "linux";
#else
  return "none";
#endif
}
