#include "agent/Buffer.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

std::size_t agent::Buffer::_roundUpPow2(std::size_t _value) noexcept
{
    if (_value < 2)
        return 2;

    std::size_t result = 1;
    while (result < _value && result <= (std::numeric_limits<std::size_t>::max() >> 1))
        result <<= 1;
    return result;
}

agent::Buffer::Buffer(std::size_t _size)
    : _capacity(_roundUpPow2(_size)),
      _mask(_capacity - 1),
      _data(new char[_capacity])
{}

// -------------------------------------------------------------------------
// Producer side
// -------------------------------------------------------------------------

std::size_t agent::Buffer::Write(const char* _input, std::size_t _size) noexcept
{
    if (_input == nullptr || _size == 0)
        return 0;

    const std::size_t head = _head.load(std::memory_order_relaxed);

    // Refresh the cached read position only if the cached view looks too full
    std::size_t space = _capacity - (head - _cachedTail);
    if (space < _size)
    {
        _cachedTail = _tail.load(std::memory_order_acquire);
        space = _capacity - (head - _cachedTail);
    }

    const std::size_t written = std::min(_size, space);
    if (written == 0)
        return 0;

    // Copy in up to two segments: [offset, end) and then [0, remainder)
    const std::size_t offset = head & _mask;
    const std::size_t first = std::min(written, _capacity - offset);
    std::memcpy(_data.get() + offset, _input, first);
    if (written > first)
        std::memcpy(_data.get(), _input + first, written - first);

    // Publish the new bytes to the consumer
    _head.store(head + written, std::memory_order_release);
    return written;
}

std::size_t agent::Buffer::Space() const noexcept
{
    const std::size_t head = _head.load(std::memory_order_relaxed);
    const std::size_t tail = _tail.load(std::memory_order_acquire);
    return _capacity - (head - tail);
}

// -------------------------------------------------------------------------
// Consumer side
// -------------------------------------------------------------------------

std::size_t agent::Buffer::Peek(char* _out, std::size_t _size) const noexcept
{
    if (_out == nullptr || _size == 0)
        return 0;

    const std::size_t tail = _tail.load(std::memory_order_relaxed);

    // Refresh the cached write position only if the cached view looks too empty
    std::size_t avail = _cachedHead - tail;
    if (avail < _size)
    {
        _cachedHead = _head.load(std::memory_order_acquire);
        avail = _cachedHead - tail;
    }

    const std::size_t count = std::min(_size, avail);
    if (count == 0)
        return 0;

    const std::size_t offset = tail & _mask;
    const std::size_t first = std::min(count, _capacity - offset);
    std::memcpy(_out, _data.get() + offset, first);
    if (count > first)
        std::memcpy(_out + first, _data.get(), count - first);

    return count;
}

std::size_t agent::Buffer::Read(char* _out, std::size_t _size) noexcept
{
    const std::size_t count = Peek(_out, _size);
    if (count > 0)
        _tail.store(_tail.load(std::memory_order_relaxed) + count, std::memory_order_release);
    return count;
}

std::pair<const char*, std::size_t> agent::Buffer::ContiguousData() const noexcept
{
    const std::size_t tail = _tail.load(std::memory_order_relaxed);
    _cachedHead = _head.load(std::memory_order_acquire);

    const std::size_t avail = _cachedHead - tail;
    const std::size_t offset = tail & _mask;
    const std::size_t contiguous = std::min(avail, _capacity - offset);
    return {_data.get() + offset, contiguous};
}

std::size_t agent::Buffer::Consume(std::size_t _size) noexcept
{
    const std::size_t tail = _tail.load(std::memory_order_relaxed);

    std::size_t avail = _cachedHead - tail;
    if (avail < _size)
    {
        _cachedHead = _head.load(std::memory_order_acquire);
        avail = _cachedHead - tail;
    }

    const std::size_t count = std::min(_size, avail);
    if (count > 0)
        _tail.store(tail + count, std::memory_order_release);
    return count;
}

void agent::Buffer::Shift(std::size_t _size) noexcept
{
    Consume(_size);
}

void agent::Buffer::Drain() noexcept
{
    _cachedHead = _head.load(std::memory_order_acquire);
    _tail.store(_cachedHead, std::memory_order_release);
}

// -------------------------------------------------------------------------
// Either side
// -------------------------------------------------------------------------

std::size_t agent::Buffer::Available() const noexcept
{
    // Load tail first: head only grows, so head - tail can never underflow
    const std::size_t tail = _tail.load(std::memory_order_acquire);
    const std::size_t head = _head.load(std::memory_order_acquire);
    return head - tail;
}
