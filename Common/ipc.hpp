#pragma once
#include "message.hpp"
#include "platform.hpp"
#include <array>
#include <deque>
namespace lga {
// Address-stable object: Win32 retains pointers into its operations and buffers.
class Channel {
    struct Operation {
        Handle event_handle = event();
        OVERLAPPED ov{};
        bool active = false, pending = false;
        DWORD bytes = 0;
        Operation() { ov.hEvent = event_handle.get(); }
        void reset() {
            ResetEvent(event_handle.get());
            ov = {};
            ov.hEvent = event_handle.get();
            active = true;
            pending = false;
            bytes = 0;
        }
    };
    Handle pipe_;
    Operation read_, write_;
    bool connecting_ = false, connected_ = false, closed_ = false;
    bool receiving_ = true;
    std::array<std::uint8_t, 4096> read_buffer_{};
    Decoder decoder_;
    std::deque<std::vector<std::uint8_t>> outgoing_;
    std::size_t write_offset_ = 0, queued_bytes_ = 0;
    std::uint64_t sent_messages_ = 0, sent_bytes_ = 0, received_messages_ = 0, received_bytes_ = 0;
    void begin_read();
    void begin_write();
    bool finish(Operation &operation);

  public:
    Channel(const std::wstring &name, bool server);
    ~Channel() { close(); }
    Channel(const Channel &) = delete;
    Channel &operator=(const Channel &) = delete;
    bool connected() const { return connected_ && !closed_; }
    bool closed() const { return closed_; }
    bool drained() const { return outgoing_.empty(); }
    std::size_t queued_messages() const { return outgoing_.size(); }
    std::uint64_t sent_messages() const { return sent_messages_; }
    std::uint64_t sent_bytes() const { return sent_bytes_; }
    std::uint64_t received_messages() const { return received_messages_; }
    std::uint64_t received_bytes() const { return received_bytes_; }
    void send(const Message &message);
    std::vector<Message> pump();
    void append_events(std::vector<HANDLE> &events) const;
    void stop_receiving() noexcept;
    void close() noexcept;
};
inline void wait_events(const std::vector<HANDLE> &handles, DWORD timeout = INFINITE) {
    if (handles.empty() || handles.size() > MAXIMUM_WAIT_OBJECTS)
        throw std::runtime_error("Invalid wait set");
    if (WaitForMultipleObjects(static_cast<DWORD>(handles.size()), handles.data(), FALSE, timeout) == WAIT_FAILED)
        throw win_error("WaitForMultipleObjects");
}
} // namespace lga
