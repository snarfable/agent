#include "agent/Buffer.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <mutex>

#include <gtest/gtest.h>

using agent::Buffer;

namespace
{
  std::string Contents(const Buffer& _buffer)
  {
    std::string out(_buffer.Available(), '\0');
    _buffer.Peek(&out[0], out.size());
    return out;
  }
}

// -------------------------------------------------------------------------
// Construction and capacity
// -------------------------------------------------------------------------

TEST(RingBuffer, CapacityRoundsUpToPowerOfTwo)
{
  EXPECT_EQ(Buffer(0).Capacity(), 2u);
  EXPECT_EQ(Buffer(1).Capacity(), 2u);
  EXPECT_EQ(Buffer(16).Capacity(), 16u);
  EXPECT_EQ(Buffer(17).Capacity(), 32u);
  EXPECT_EQ(Buffer(1000).Capacity(), 1024u);
}

TEST(RingBuffer, DefaultCapacityMatchesConfig)
{
  Buffer buffer;
  EXPECT_GE(buffer.Capacity(), static_cast<std::size_t>(AGENT_CONN_BUFFER_SIZE));
  EXPECT_TRUE(buffer.Empty());
  EXPECT_EQ(buffer.Space(), buffer.Capacity());
}

// -------------------------------------------------------------------------
// Basic single-threaded semantics
// -------------------------------------------------------------------------

TEST(RingBuffer, WriteThenReadRoundTrips)
{
  Buffer buffer(64);
  const std::string data = "Random test data";

  EXPECT_EQ(buffer.Write(data.data(), data.size()), data.size());
  EXPECT_EQ(buffer.Available(), data.size());
  EXPECT_EQ(Contents(buffer), data);

  std::string out(data.size(), '\0');
  EXPECT_EQ(buffer.Read(&out[0], out.size()), data.size());
  EXPECT_EQ(out, data);
  EXPECT_TRUE(buffer.Empty());
}

TEST(RingBuffer, AppendsInsteadOfOverwriting)
{
  // Regression: the previous linear Buffer copied every write to offset 0
  Buffer buffer(64);
  buffer.Write("abc", 3);
  buffer.Write("def", 3);
  EXPECT_EQ(Contents(buffer), "abcdef");
}

TEST(RingBuffer, NullOrZeroLengthWritesAreNoOps)
{
  Buffer buffer(16);
  EXPECT_EQ(buffer.Write(nullptr, 5), 0u);
  EXPECT_EQ(buffer.Write("x", 0), 0u);
  EXPECT_TRUE(buffer.Empty());
}

TEST(RingBuffer, WriteStopsWhenFull)
{
  Buffer buffer(8);
  EXPECT_EQ(buffer.Write("0123456789", 10), 8u);
  EXPECT_TRUE(buffer.Full());
  EXPECT_EQ(buffer.Space(), 0u);
  EXPECT_EQ(buffer.Write("z", 1), 0u);
  EXPECT_EQ(Contents(buffer), "01234567");
}

TEST(RingBuffer, FillsToExactCapacity)
{
  // Regression: the previous Buffer refused a write that exactly filled it
  Buffer buffer(8);
  EXPECT_EQ(buffer.Write("01234567", 8), 8u);
  EXPECT_TRUE(buffer.Full());
}

TEST(RingBuffer, PeekDoesNotConsume)
{
  Buffer buffer(16);
  buffer.Write("hello", 5);
  char out[5];
  EXPECT_EQ(buffer.Peek(out, 5), 5u);
  EXPECT_EQ(buffer.Peek(out, 5), 5u);
  EXPECT_EQ(buffer.Available(), 5u);
}

TEST(RingBuffer, ReadMoreThanAvailableIsClamped)
{
  Buffer buffer(16);
  buffer.Write("abc", 3);
  char out[16];
  EXPECT_EQ(buffer.Read(out, sizeof(out)), 3u);
  EXPECT_EQ(buffer.Read(out, sizeof(out)), 0u);
}

class RingBufferShift : public ::testing::TestWithParam<int> {};

TEST_P(RingBufferShift, ShiftAdvancesReadPosition)
{
  Buffer buffer(64);
  const std::string data = "Random test data";
  buffer.Write(data.data(), data.size());

  buffer.Shift(GetParam());
  EXPECT_EQ(buffer.Available(), data.size() - GetParam());
  EXPECT_EQ(Contents(buffer), data.substr(GetParam()));
}

INSTANTIATE_TEST_SUITE_P(RingBufferShiftSuite, RingBufferShift, ::testing::Values(1, 2, 3, 16));

TEST(RingBuffer, ConsumeIsClampedToAvailable)
{
  Buffer buffer(16);
  buffer.Write("abc", 3);
  EXPECT_EQ(buffer.Consume(100), 3u);
  EXPECT_TRUE(buffer.Empty());
}

TEST(RingBuffer, DrainDiscardsEverything)
{
  Buffer buffer(16);
  buffer.Write("abcdef", 6);
  buffer.Drain();
  EXPECT_TRUE(buffer.Empty());
  EXPECT_EQ(buffer.Space(), buffer.Capacity());
}

// -------------------------------------------------------------------------
// Wrap-around behaviour
// -------------------------------------------------------------------------

TEST(RingBuffer, WritesAndReadsAcrossTheWrapPoint)
{
  Buffer buffer(8);
  buffer.Write("012345", 6);
  buffer.Consume(5);                 // read position now at offset 5
  EXPECT_EQ(buffer.Write("abcdefg", 7), 7u);  // wraps: 3 bytes at end, 4 at start
  EXPECT_TRUE(buffer.Full());
  EXPECT_EQ(Contents(buffer), "5abcdefg");
}

TEST(RingBuffer, ContiguousDataStopsAtWrapPoint)
{
  Buffer buffer(8);
  buffer.Write("012345", 6);
  buffer.Consume(6);                 // offset 6, empty
  buffer.Write("abcde", 5);          // "ab" at 6..7, "cde" at 0..2

  auto view = buffer.ContiguousData();
  EXPECT_EQ(view.second, 2u);
  EXPECT_EQ(std::string(view.first, view.second), "ab");
  EXPECT_EQ(buffer.Available(), 5u);

  buffer.Consume(view.second);
  view = buffer.ContiguousData();
  EXPECT_EQ(std::string(view.first, view.second), "cde");
}

TEST(RingBuffer, ContiguousDataOnEmptyBuffer)
{
  Buffer buffer(8);
  EXPECT_EQ(buffer.ContiguousData().second, 0u);
}

TEST(RingBuffer, RandomizedOperationsMatchReferenceModel)
{
  // Compare against a trivially correct std::string model over many
  // wrap-arounds with random write and read sizes.
  Buffer buffer(64);
  std::string model;
  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> size(0, 80);
  std::uniform_int_distribution<int> op(0, 3);
  std::uint8_t next = 0;

  for (int i = 0; i < 20000; ++i)
  {
    const int n = size(rng);
    switch (op(rng))
    {
      case 0:
      case 1:
      {
        std::string chunk(n, '\0');
        for (auto& c : chunk) c = static_cast<char>(next++);
        const std::size_t w = buffer.Write(chunk.data(), chunk.size());
        const std::size_t expected = std::min<std::size_t>(chunk.size(), 64 - model.size());
        ASSERT_EQ(w, expected);
        model.append(chunk, 0, w);
        next = static_cast<std::uint8_t>(next - (chunk.size() - w));
        break;
      }
      case 2:
      {
        std::string out(n, '\0');
        const std::size_t r = buffer.Read(&out[0], out.size());
        ASSERT_EQ(r, std::min<std::size_t>(n, model.size()));
        ASSERT_EQ(out.substr(0, r), model.substr(0, r));
        model.erase(0, r);
        break;
      }
      case 3:
      {
        const std::size_t c = buffer.Consume(n);
        ASSERT_EQ(c, std::min<std::size_t>(n, model.size()));
        model.erase(0, c);
        break;
      }
    }
    ASSERT_EQ(buffer.Available(), model.size());
    ASSERT_EQ(Contents(buffer), model);
  }
}

// -------------------------------------------------------------------------
// Concurrency: one producer thread, one consumer thread
// -------------------------------------------------------------------------

TEST(RingBufferConcurrency, SpscStreamArrivesIntactAndInOrder)
{
  // A small buffer forces constant wrap-around and full/empty transitions.
  Buffer buffer(256);
  constexpr std::size_t total = 8u * 1024u * 1024u;

  std::thread producer([&]() {
    std::mt19937 rng(1);
    std::uniform_int_distribution<int> chunk(1, 97);
    std::vector<char> scratch(97);
    std::size_t sent = 0;
    while (sent < total)
    {
      const std::size_t n = std::min<std::size_t>(chunk(rng), total - sent);
      for (std::size_t i = 0; i < n; ++i)
        scratch[i] = static_cast<char>((sent + i) & 0xFF);
      std::size_t done = 0;
      while (done < n)
      {
        const std::size_t w = buffer.Write(scratch.data() + done, n - done);
        if (w == 0) std::this_thread::yield();
        done += w;
      }
      sent += n;
    }
  });

  std::size_t received = 0;
  std::size_t mismatches = 0;
  std::mt19937 rng(2);
  std::uniform_int_distribution<int> chunk(1, 113);
  std::vector<char> out(113);
  while (received < total)
  {
    const std::size_t r = buffer.Read(out.data(), chunk(rng));
    if (r == 0) { std::this_thread::yield(); continue; }
    for (std::size_t i = 0; i < r; ++i)
      if (out[i] != static_cast<char>((received + i) & 0xFF))
        ++mismatches;
    received += r;
  }

  producer.join();
  EXPECT_EQ(received, total);
  EXPECT_EQ(mismatches, 0u);
  EXPECT_TRUE(buffer.Empty());
}

TEST(RingBufferConcurrency, ZeroCopyConsumerSeesIntactStream)
{
  // Same as above, but the consumer uses ContiguousData() + Consume(), the
  // path used by IConnectionHandler to feed AMQP::Connection::parse().
  Buffer buffer(128);
  constexpr std::size_t total = 4u * 1024u * 1024u;

  std::thread producer([&]() {
    std::vector<char> scratch(61);
    std::size_t sent = 0;
    while (sent < total)
    {
      const std::size_t n = std::min<std::size_t>(scratch.size(), total - sent);
      for (std::size_t i = 0; i < n; ++i)
        scratch[i] = static_cast<char>((sent + i) * 31u);
      std::size_t done = 0;
      while (done < n)
      {
        const std::size_t w = buffer.Write(scratch.data() + done, n - done);
        if (w == 0) std::this_thread::yield();
        done += w;
      }
      sent += n;
    }
  });

  std::size_t received = 0;
  std::size_t mismatches = 0;
  while (received < total)
  {
    const auto view = buffer.ContiguousData();
    if (view.second == 0) { std::this_thread::yield(); continue; }
    for (std::size_t i = 0; i < view.second; ++i)
      if (view.first[i] != static_cast<char>((received + i) * 31u))
        ++mismatches;
    received += buffer.Consume(view.second);
  }

  producer.join();
  EXPECT_EQ(received, total);
  EXPECT_EQ(mismatches, 0u);
}

TEST(RingBufferConcurrency, MutexSerializedProducersNeverInterleaveRecords)
{
  // Mirrors IConnectionHandler::onData(): several threads produce into one
  // ring through a mutex, each queueing a whole record at once (or releasing
  // the lock and retrying), while a single lock-free consumer drains it.
  // Every record must arrive intact, and each producer's records in order.
  Buffer buffer(512);
  std::mutex producerLock;
  constexpr int producers = 4;
  constexpr std::uint32_t recordsPerProducer = 20000;

  auto produce = [&](std::uint8_t id) {
    std::mt19937 rng(id);
    std::uniform_int_distribution<int> payload(0, 40);
    std::vector<char> record;
    for (std::uint32_t seq = 0; seq < recordsPerProducer; ++seq)
    {
      // Record layout: [id][seq:4][len:1][payload bytes all equal to id]
      const std::uint8_t len = static_cast<std::uint8_t>(payload(rng));
      record.assign(6 + len, static_cast<char>(id));
      std::memcpy(record.data() + 1, &seq, sizeof(seq));
      record[5] = static_cast<char>(len);
      for (;;)
      {
        {
          std::lock_guard<std::mutex> lock(producerLock);
          if (buffer.Space() >= record.size())
          {
            buffer.Write(record.data(), record.size());
            break;
          }
        }
        std::this_thread::yield();
      }
    }
  };

  std::vector<std::thread> threads;
  for (int p = 0; p < producers; ++p)
    threads.emplace_back(produce, static_cast<std::uint8_t>(p));

  std::vector<std::uint32_t> nextSeq(producers, 0);
  std::size_t corrupt = 0;
  std::size_t received = 0;
  char header[6];
  char body[64];
  while (received < static_cast<std::size_t>(producers) * recordsPerProducer)
  {
    if (buffer.Peek(header, sizeof(header)) < sizeof(header))
    {
      std::this_thread::yield();
      continue;
    }
    const std::uint8_t len = static_cast<std::uint8_t>(header[5]);
    if (buffer.Available() < sizeof(header) + len)
    {
      std::this_thread::yield();
      continue;
    }
    buffer.Consume(sizeof(header));
    buffer.Read(body, len);

    const std::uint8_t id = static_cast<std::uint8_t>(header[0]);
    std::uint32_t seq;
    std::memcpy(&seq, header + 1, sizeof(seq));
    if (id >= producers || seq != nextSeq[id]++)
      ++corrupt;
    for (std::uint8_t i = 0; i < len; ++i)
      if (static_cast<std::uint8_t>(body[i]) != id)
        ++corrupt;
    ++received;
  }

  for (auto& t : threads)
    t.join();
  EXPECT_EQ(corrupt, 0u);
  EXPECT_TRUE(buffer.Empty());
}
