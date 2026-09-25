/**
 * @file buffer_bench.cpp
 * @brief Throughput benchmark for agent::Buffer (SPSC ring buffer)
 *
 * Streams a fixed number of bytes from a producer thread to a consumer thread
 * through agent::Buffer for several chunk sizes and reports throughput.
 * Build with optimizations (e.g. -DCMAKE_BUILD_TYPE=Release).
 *
 * Usage: buffer_bench [total_megabytes] [buffer_kilobytes]
 */
#include "agent/Buffer.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

namespace
{
  double RunOnce(std::size_t _total, std::size_t _chunk, std::size_t _capacity)
  {
    agent::Buffer buffer(_capacity);
    std::vector<char> in(_chunk, 'x');
    std::vector<char> out(_chunk);

    const auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
      std::size_t sent = 0;
      while (sent < _total)
      {
        const std::size_t want = std::min(_chunk, _total - sent);
        const std::size_t w = buffer.Write(in.data(), want);
        if (w == 0)
          std::this_thread::yield();
        sent += w;
      }
    });

    std::size_t received = 0;
    while (received < _total)
    {
      const std::size_t r = buffer.Read(out.data(), out.size());
      if (r == 0)
        std::this_thread::yield();
      received += r;
    }
    producer.join();

    const std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
    return static_cast<double>(_total) / elapsed.count() / (1024.0 * 1024.0 * 1024.0);
  }
}

int main(int argc, char** argv)
{
  const std::size_t totalMiB = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2048;
  const std::size_t bufferKiB = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1024;
  const std::size_t total = totalMiB * 1024 * 1024;
  const std::size_t capacity = bufferKiB * 1024;

  std::printf("agent::Buffer SPSC throughput: %zu MiB through a %zu KiB ring\n", totalMiB, bufferKiB);
  std::printf("%12s %14s\n", "chunk (B)", "GiB/s");
  for (std::size_t chunk : {64u, 256u, 1024u, 4096u, 16384u, 65536u})
  {
    double best = 0.0;
    for (int trial = 0; trial < 3; ++trial)
      best = std::max(best, RunOnce(total, chunk, capacity));
    std::printf("%12zu %14.2f\n", chunk, best);
  }
  return 0;
}
