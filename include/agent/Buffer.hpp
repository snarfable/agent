#pragma once

#include <agent/agent.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace agent
{
	/**
	 * @brief Lock-free single-producer/single-consumer (SPSC) byte ring buffer
	 *
	 * The buffer stores raw bytes in a fixed-capacity circular array. Capacity
	 * is rounded up to the next power of two so that index wrapping is a single
	 * bitwise AND. The read and write positions are monotonically increasing
	 * counters (they never wrap themselves), which makes the full/empty cases
	 * unambiguous: the number of readable bytes is always \c head - \c tail.
	 *
	 * Thread-safety contract:
	 *  - Exactly one thread may act as the producer and call \c Write() and
	 *    \c Space().
	 *  - Exactly one thread may act as the consumer and call \c Peek(),
	 *    \c Read(), \c ContiguousData(), \c Consume(), \c Shift() and
	 *    \c Drain().
	 *  - \c Available(), \c Capacity(), \c Empty() and \c Full() may be called
	 *    from either side; they return a consistent snapshot.
	 *  - The producer and the consumer may be the same thread.
	 *
	 * Synchronization uses acquire/release ordering only. The producer
	 * publishes bytes with a release store to \c _head after copying them in,
	 * and the consumer frees space with a release store to \c _tail after it
	 * has finished with the bytes. Each side keeps a cached copy of the other
	 * side's index so that the shared cache line is only touched when the
	 * cached value says the buffer looks full (producer) or empty (consumer).
	 */
	class Buffer
	{
	public:
		/**
		 * @brief Construct a new Buffer object
		 *
		 * @param _size Requested capacity in bytes; rounded up to a power of two
		 */
		explicit Buffer(std::size_t _size = AGENT_CONN_BUFFER_SIZE);
		~Buffer() = default;

		Buffer(const Buffer&) = delete;
		Buffer& operator=(const Buffer&) = delete;
		Buffer(Buffer&&) = delete;
		Buffer& operator=(Buffer&&) = delete;

		// ---------------------------------------------------------------
		// Producer side
		// ---------------------------------------------------------------

		/**
		 * @brief Write data into the buffer (producer only)
		 *
		 * Copies as many bytes as currently fit and never overwrites unread
		 * data.
		 *
		 * @param _data Pointer to the data to be written
		 * @param _size The number of bytes to be written
		 * @return std::size_t Number of bytes actually written
		 */
		std::size_t Write(const char* _data, std::size_t _size) noexcept;

		/**
		 * @brief Number of bytes that can currently be written (producer only)
		 *
		 * @return std::size_t Free space in bytes
		 */
		std::size_t Space() const noexcept;

		// ---------------------------------------------------------------
		// Consumer side
		// ---------------------------------------------------------------

		/**
		 * @brief Copy up to \c _size readable bytes without consuming them
		 *
		 * @param _out Destination for the bytes
		 * @param _size Maximum number of bytes to copy
		 * @return std::size_t Number of bytes copied
		 */
		std::size_t Peek(char* _out, std::size_t _size) const noexcept;

		/**
		 * @brief Copy up to \c _size readable bytes and consume them
		 *
		 * @param _out Destination for the bytes
		 * @param _size Maximum number of bytes to read
		 * @return std::size_t Number of bytes read
		 */
		std::size_t Read(char* _out, std::size_t _size) noexcept;

		/**
		 * @brief Zero-copy view of the readable bytes up to the wrap point
		 *
		 * Returns a pointer to the oldest unread byte and the number of bytes
		 * that are contiguous in memory from there. If the readable region
		 * wraps around the end of the storage, the returned length is smaller
		 * than \c Available(); use \c Peek() to linearize in that case.
		 *
		 * @return std::pair<const char*, std::size_t> Pointer and length
		 */
		std::pair<const char*, std::size_t> ContiguousData() const noexcept;

		/**
		 * @brief Mark \c _size bytes as consumed, freeing their space
		 *
		 * Requests larger than \c Available() are clamped.
		 *
		 * @param _size Number of bytes to consume
		 * @return std::size_t Number of bytes actually consumed
		 */
		std::size_t Consume(std::size_t _size) noexcept;

		/**
		 * @brief Alias for \c Consume() kept for API compatibility
		 *
		 * @param _size Number of bytes by which to advance the read position
		 */
		void Shift(std::size_t _size) noexcept;

		/**
		 * @brief Discard everything currently readable (consumer only)
		 */
		void Drain() noexcept;

		// ---------------------------------------------------------------
		// Either side
		// ---------------------------------------------------------------

		/**
		 * @brief Requests the number of bytes available to read
		 *
		 * @return std::size_t Number of bytes available
		 */
		std::size_t Available() const noexcept;

		/**
		 * @brief Total capacity in bytes (a power of two)
		 */
		std::size_t Capacity() const noexcept { return _capacity; }

		bool Empty() const noexcept { return Available() == 0; }
		bool Full() const noexcept { return Available() == _capacity; }

	private:
		static std::size_t _roundUpPow2(std::size_t _value) noexcept;

		// Destructive interference size; 64 bytes covers x86-64 and most ARM
		static constexpr std::size_t CacheLine = 64;

		const std::size_t _capacity;
		const std::size_t _mask;
		const std::unique_ptr<char[]> _data;

		// Producer-owned line: write position plus cached read position
		alignas(CacheLine) std::atomic<std::size_t> _head{0};
		std::size_t _cachedTail{0};

		// Consumer-owned line: read position plus cached write position.
		// alignas also pads sizeof(Buffer) to a multiple of the cache line, so
		// adjacent objects cannot share this line.
		alignas(CacheLine) std::atomic<std::size_t> _tail{0};
		mutable std::size_t _cachedHead{0};
	};
}
