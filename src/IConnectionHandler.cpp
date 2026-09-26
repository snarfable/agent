#include "agent/agent.hpp"

#include "agent/IConnectionHandler.hpp"
#include "agent/Buffer.hpp"
#include "agent/IWorker.hpp"

#include <algorithm>
#include <mutex>
#include <thread>

#include <amqpcpp.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <cstdint>
#include <string>
#include <sstream>

agent::IConnectionHandler::IConnectionHandler(unsigned int _id)
    : _client("IConnectionHandler"), // Default client name
      _connected(false),
      _connection(nullptr),
      _inpbuffer(AGENT_CONN_BUFFER_SIZE),
      _tmpbuffer(AGENT_CONN_TEMP_BUFFER_SIZE),
      _outbuffer(AGENT_CONN_BUFFER_SIZE),
      _address(Poco::Net::SocketAddress("localhost", 5672)),
      _logger(nullptr), // Default no logger
      IWorker(_id)
{
  // Check if logger called GetName() exists, else create it
  _logger = spdlog::get(_client);
  if (_logger == nullptr)
    _logger = spdlog::stdout_color_mt(_client);

  // Just announce the creation of the client; can turn this off via log level
  _logger->info("Client {} created", _client);

  // Set up the AMQP::Connection here and then Run()
  _socket.connect(_address);
  _socket.setKeepAlive(true);
}

agent::IConnectionHandler::IConnectionHandler(
    unsigned int _id,
    const std::string& _host,
    std::uint16_t _port,
    const std::string& _name,
    const std::string& __product,
    const std::string& __version,
    const std::string& __copyright,
    const std::string& __information
  )
    : _client(_name),
      _product(__product),
      _version(__version),
      _copyright(__copyright),
      _information(__information),
      _connected(false),
      _connection(nullptr),
      _inpbuffer(AGENT_CONN_BUFFER_SIZE),
      _tmpbuffer(AGENT_CONN_TEMP_BUFFER_SIZE),
      _outbuffer(AGENT_CONN_BUFFER_SIZE),
      _address(Poco::Net::SocketAddress(_host, _port)),
      IWorker(_id, _name)
{
  // Check if logger called GetName() exists, else create it
  _logger = spdlog::get(_client);
  if (_logger == nullptr)
    _logger = spdlog::stdout_color_mt(_client);
  
  // Just announce the creation of the client; can turn this off via log level
  _logger->info("Client {} created", _client);

  // Set up the AMQP::Connection here and then Run()
  _socket.connect(_address);
  _socket.setKeepAlive(true);
}

void agent::IConnectionHandler::onProperties(AMQP::Connection *__connection, const AMQP::Table &_server, AMQP::Table &__client)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Make sure we know who you are
  __client["connection_name"] = _client;
  __client["product"] = _product;
  __client["version"] = _version;
  __client["copyright"] = _copyright;
  __client["information"] = _information;

  // Set the platform
  #if defined(__linux__)
  __client["platform"] = "Linux";
  #elif defined(__APPLE__)
  __client["platform"] = "Mac OS X";
  #elif defined(__sun)
  __client["platform"] = "Solaris";
  #elif defined(__FreeBSD__)
  __client["platform"] = "FreeBSD";
  #elif defined(__OpenBSD__)
  __client["platform"] = "OpenBSD";
  #elif defined(__NetBSD__)
  __client["platform"] = "NetBSD";
  #elif defined(__hpux)
  __client["platform"] = "HP-UX";
  #elif defined(__osf__)
  __client["platform"] = "Tru64 UNIX";
  #elif defined(__sgi)
  __client["platform"] = "Irix";
  #elif defined(_AIX)
  __client["platform"] = "AIX";
  #elif defined(_WIN32)
  __client["platform"] = "Windows";
  #endif

  // Print details of the client and server
  auto _clientss = std::ostringstream();
  auto _serverss = std::ostringstream();

  _clientss << _client;
  _serverss << _server;

  _logger->info("[onProperties] Client: {}, Server: {}", _clientss.str(), _serverss.str());
}

uint16_t agent::IConnectionHandler::onNegotiate(AMQP::Connection *__connection, uint16_t _interval)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Print details of the heartbeat negotiation
  _logger->info("[onNegotiate] Accepting interval of length {}", _interval);

  // Just accept the interval
  return _interval;
}

void agent::IConnectionHandler::onData(AMQP::Connection *__connection, const char *_data, size_t _size)
{
  if (_connection == nullptr)
    _connection = __connection;

  if (_data == nullptr || _size == 0)
    return;

  // A single call larger than the ring could never be queued contiguously
  if (_size > _outbuffer.Capacity())
  {
    _logger->error("[onData] {} bytes exceeds output buffer capacity {}; dropping", _size, _outbuffer.Capacity());
    return;
  }

  const bool onLoopThread = std::this_thread::get_id() == _loopThread.load(std::memory_order_acquire);

  if (onLoopThread)
  {
    // The loop thread is also the ring's only consumer, so when the ring is
    // full it can make room itself by flushing to the socket.
    std::lock_guard<std::mutex> lock(_outmutex);
    std::size_t queued = 0;
    while (queued < _size)
    {
      queued += _outbuffer.Write(_data + queued, _size - queued);
      if (queued < _size && !_sendDataFromBuffer())
      {
        _logger->error("[onData] Socket send failed; dropped {} of {} bytes", _size - queued, _size);
        return;
      }
    }
  }
  else
  {
    // Other threads must never wait while holding the lock, or the loop
    // thread could block in its own onData() and stop draining the ring.
    // Queue the whole call at once, or release the lock and retry.
    for (;;)
    {
      {
        std::lock_guard<std::mutex> lock(_outmutex);
        if (_outbuffer.Space() >= _size)
        {
          _outbuffer.Write(_data, _size);
          break;
        }
      }
      if (GetState() == WORKER_QUIT)
      {
        _logger->error("[onData] Worker quitting; dropped {} bytes", _size);
        return;
      }
      std::this_thread::yield();
    }
  }

  _logger->debug("[onData] Queued {} bytes", _size);
}

void agent::IConnectionHandler::onHeartbeat(AMQP::Connection *__connection)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Announce that we received a heartbeat from the AMQP server
  _logger->debug("[onHeartbeat] Received a heartbeat from server");

  // Send a return heartbeat; this isn't ideal because it depends on receiving a
  // heartbeat first, which might not happen. Set up an independent thread that
  // takes care of this
  _connection->heartbeat();
}

void agent::IConnectionHandler::onError(AMQP::Connection *__connection, const char *_message)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Announce that we received a heartbeat from the AMQP server
  _logger->error("[onError] Error: {}", _message);
}

void agent::IConnectionHandler::onReady(AMQP::Connection *__connection)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Notify the log that the connection is read
  _logger->info("[onReady] Connection is ready");
}

void agent::IConnectionHandler::onClosed(AMQP::Connection *__connection)
{
  if (_connection == nullptr)
    _connection = __connection;

  // Announce and close the connection
  _logger->info("[onClosed] Connection closed");

  // Exit the loop
  quit();
}

// This function should not exist at all; why can't I get rid of it?
int agent::IConnectionHandler::ProcessMessage(const void*_msg, std::uint32_t _size, void*_result, std::uint32_t*_rsize)
{
  return 0;
}

// Needed for handling conversion of StreamSocket to SecureStreamSocket
Poco::Net::StreamSocket& agent::IConnectionHandler::socket()
{
  return _socket;
}

void agent::IConnectionHandler::operator()()
{
  // Record the consumer thread for onData()
  _loopThread.store(std::this_thread::get_id(), std::memory_order_release);

  // This is the main worker loop for AMQP transactions
  if (_socket.secure())
  {
    // Debugging info; indicate we're in TLS mode
    _logger->debug("Connection is secure");

    // We need to ignore _socket.available() in secure mode; it always returns 0
    while (GetState() != WORKER_QUIT)
    {
      /**
       * Important: Because we can't use _socket.available() becuase it doesn't
       * work for TLS, we need to have another way of sizing the buffer in the
       * case of a large amount of incoming data.
       */
      
      // Make sure all bytes read were processed
      // Never read more than the ring buffer can currently hold
      const std::size_t space = std::min(_tmpbuffer.size(), _inpbuffer.Space());
      if (space > 0)
      {
        const int rbytes = _socket.receiveBytes(_tmpbuffer.data(), static_cast<int>(space));
        if (rbytes < 0)
          _logger->error("Socket error: receiveBytes returned {}", rbytes);
        else if (rbytes > 0)
          _inpbuffer.Write(_tmpbuffer.data(), static_cast<std::size_t>(rbytes));
      }

      _parseInputBuffer();
      _sendDataFromBuffer();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  else
  {
    // Debugging info
    _logger->debug("Connection is not secure");

    // If we're not in TLS mode, we can use _socket.available()
    while (GetState() != WORKER_QUIT)
    {
      // See if there's any data available on the incoming socket
      const int savail = _socket.available();
      if (savail > 0)
      {
        // Never read more than the ring buffer can currently hold; anything
        // left over stays in the socket until the next pass
        const std::size_t space = std::min({static_cast<std::size_t>(savail), _tmpbuffer.size(), _inpbuffer.Space()});
        if (space > 0)
        {
          const int rbytes = _socket.receiveBytes(_tmpbuffer.data(), static_cast<int>(space));
          if (rbytes < 0)
            _logger->error("Socket error: receiveBytes returned {}", rbytes);
          else if (rbytes > 0)
            _inpbuffer.Write(_tmpbuffer.data(), static_cast<std::size_t>(rbytes));
        }
      }
      else if (savail < 0)
      {
        _logger->error("Socket error: Available bytes on socket < 0");
      }

      _parseInputBuffer();
      _sendDataFromBuffer();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Flush anything still queued before the loop exits
  if (GetState() == WORKER_QUIT && _outbuffer.Available())
    _sendDataFromBuffer();

  _loopThread.store(std::thread::id(), std::memory_order_release);
}

void agent::IConnectionHandler::quit()
{
  SetQuit();
}

bool agent::IConnectionHandler::_sendDataFromBuffer()
{
  // Consumer side of _outbuffer; only the loop thread calls this. Send in
  // contiguous runs (two when the pending bytes wrap the ring) and release
  // only what the socket accepted.
  for (;;)
  {
    const auto pending = _outbuffer.ContiguousData();
    if (pending.second == 0)
      return true;

    const int sent = _socket.sendBytes(pending.first, static_cast<int>(pending.second));
    if (sent <= 0)
    {
      _logger->error("Socket send failed: sendBytes returned {}", sent);
      return false;
    }

    _outbuffer.Consume(static_cast<std::size_t>(sent));
    _logger->debug("Sent [{:6d} / {:6d}] bytes from buffer", sent, pending.second);
  }
}

void agent::IConnectionHandler::_parseInputBuffer()
{
  if (_connection == nullptr)
    return;

  const std::size_t iavail = _inpbuffer.Available();
  if (iavail == 0)
    return;

  // Zero-copy fast path: hand AMQP-CPP the bytes directly out of the ring
  auto view = _inpbuffer.ContiguousData();

  // The readable bytes wrap around the end of the ring. AMQP-CPP needs whole
  // frames in contiguous memory, so linearize into scratch space first.
  if (view.second < iavail)
  {
    if (_parsebuffer.size() < iavail)
      _parsebuffer.resize(iavail);
    view.second = _inpbuffer.Peek(_parsebuffer.data(), iavail);
    view.first = _parsebuffer.data();
  }

  const std::size_t parsed = _connection->parse(view.first, view.second);
  _inpbuffer.Consume(parsed);
}
