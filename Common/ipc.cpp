#include "ipc.hpp"
namespace lga {
Channel::Channel(const std::wstring &name, bool server) {
    if (server) {
        pipe_.reset(CreateNamedPipeW(
            name.c_str(), PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS, 1, 4096, 4096, 0, nullptr));
        if (!pipe_)
            throw win_error("CreateNamedPipe");
        connecting_ = true;
        read_.reset();
        if (ConnectNamedPipe(pipe_.get(), &read_.ov))
            SetEvent(read_.event_handle.get());
        else {
            DWORD e = GetLastError();
            if (e == ERROR_IO_PENDING)
                read_.pending = true;
            else if (e == ERROR_PIPE_CONNECTED)
                SetEvent(read_.event_handle.get());
            else
                throw win_error("ConnectNamedPipe", e);
        }
    } else {
        const ULONGLONG deadline = GetTickCount64() + 5000;
        while (true) {
            pipe_.reset(CreateFileW(name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_EXISTING,
                                    FILE_FLAG_OVERLAPPED, nullptr));
            if (pipe_)
                break;
            DWORD e = GetLastError();
            if (e != ERROR_PIPE_BUSY && e != ERROR_FILE_NOT_FOUND)
                throw win_error("Open pipe", e);
            if (GetTickCount64() >= deadline)
                throw std::runtime_error("Pipe connection timed out after 5s");
            WaitNamedPipeW(name.c_str(), 100);
            Sleep(10);
        }
        connected_ = true;
        begin_read();
    }
}
void Channel::begin_read() {
    if (!receiving_)
        return;
    read_.reset();
    if (ReadFile(pipe_.get(), read_buffer_.data(), static_cast<DWORD>(read_buffer_.size()), &read_.bytes, &read_.ov))
        SetEvent(read_.event_handle.get());
    else {
        DWORD e = GetLastError();
        if (e == ERROR_IO_PENDING)
            read_.pending = true;
        else {
            read_.active = false;
            close();
            throw win_error("Pipe read", e);
        }
    }
}
void Channel::begin_write() {
    if (write_.active || outgoing_.empty() || !connected())
        return;
    write_.reset();
    const auto &frame = outgoing_.front();
    if (WriteFile(pipe_.get(), frame.data() + write_offset_, static_cast<DWORD>(frame.size() - write_offset_),
                  &write_.bytes, &write_.ov))
        SetEvent(write_.event_handle.get());
    else {
        DWORD e = GetLastError();
        if (e == ERROR_IO_PENDING)
            write_.pending = true;
        else {
            write_.active = false;
            close();
            throw win_error("Pipe write", e);
        }
    }
}
bool Channel::finish(Operation &op) {
    if (!op.active || WaitForSingleObject(op.event_handle.get(), 0) != WAIT_OBJECT_0)
        return false;
    if (op.pending && !GetOverlappedResult(pipe_.get(), &op.ov, &op.bytes, FALSE)) {
        DWORD e = GetLastError();
        op.pending = false;
        op.active = false;
        close();
        throw win_error("Pipe completion", e);
    }
    op.active = false;
    op.pending = false;
    ResetEvent(op.event_handle.get());
    return true;
}
std::vector<Message> Channel::pump() {
    if (closed_)
        return {};
    std::vector<Message> messages;
    // Collect local writes before starting the next read. The peer can consume
    // a request, reply and close in one scheduling turn; starting a read first
    // could observe EOF and cancel already-successful write bookkeeping.
    if (finish(write_)) {
        if (!write_.bytes) {
            close();
            throw std::runtime_error("Zero-byte write");
        }
        write_offset_ += write_.bytes;
        if (write_offset_ == outgoing_.front().size()) {
            queued_bytes_ -= outgoing_.front().size();
            outgoing_.pop_front();
            write_offset_ = 0;
        }
    }
    if (finish(read_)) {
        if (connecting_) {
            connecting_ = false;
            connected_ = true;
        } else {
            if (!read_.bytes) {
                close();
                throw std::runtime_error("Pipe peer disconnected");
            }
            messages = decoder_.feed(read_buffer_.data(), read_.bytes);
            received_bytes_ += read_.bytes;
            received_messages_ += messages.size();
        }
        // Preserve already decoded messages if the peer closes immediately
        // after its final write (notably SHUTDOWN_ACK).
        try {
            begin_read();
        } catch (...) {
            if (messages.empty())
                throw;
        }
    }
    begin_write();
    return messages;
}
void Channel::send(const Message &m) {
    if (!connected())
        throw std::runtime_error("Cannot send to disconnected pipe");
    auto frame = encode(m);
    if (outgoing_.size() >= 64 || queued_bytes_ + frame.size() > 4 * (max_payload + 12))
        throw std::runtime_error("Outgoing queue limit exceeded");
    ++sent_messages_;
    sent_bytes_ += frame.size();
    queued_bytes_ += frame.size();
    outgoing_.push_back(std::move(frame));
    begin_write();
}
void Channel::append_events(std::vector<HANDLE> &events) const {
    if (closed_)
        return;
    if (read_.active)
        events.push_back(read_.event_handle.get());
    if (write_.active)
        events.push_back(write_.event_handle.get());
}
void Channel::stop_receiving() noexcept {
    // A shutdown peer may close as soon as it consumes our acknowledgement.
    // Cancel/collect the independent read before draining the final write, so
    // an expected EOF cannot hide that write's successful completion.
    receiving_ = false;
    if (pipe_ && read_.active && read_.pending) {
        CancelIoEx(pipe_.get(), &read_.ov);
        DWORD ignored = 0;
        GetOverlappedResult(pipe_.get(), &read_.ov, &ignored, TRUE);
    }
    read_.active = false;
    read_.pending = false;
    ResetEvent(read_.event_handle.get());
}
void Channel::close() noexcept {
    if (closed_)
        return;
    closed_ = true;
    connected_ = false;
    if (pipe_) {
        for (Operation *op : {&read_, &write_}) {
            if (op->active && op->pending) {
                CancelIoEx(pipe_.get(), &op->ov);
                DWORD ignored = 0;
                GetOverlappedResult(pipe_.get(), &op->ov, &ignored, TRUE);
            }
            op->active = false;
            op->pending = false;
        }
        pipe_.reset();
    }
}
} // namespace lga
