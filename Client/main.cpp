#include "event_log.hpp"
#include "ipc.hpp"
#include <atomic>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <tuple>
using namespace lga;
namespace {
// Only stdin blocks here; all transport ownership remains on the main event loop.
class Input {
    Handle ready_ = event(), thread_;
    std::mutex mutex_;
    std::deque<std::string> lines_;
    std::atomic<bool> stop_{false};
    bool eof_ = false;
    std::string error_;
    // Called with mutex_ held for both newline-terminated commands and EOF.
    void enqueue(std::string line) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        if (line.size() > 8192)
            throw std::runtime_error("Command exceeds 8192 bytes");
        if (lines_.size() >= 128)
            throw std::runtime_error("Input queue limit (128 commands)");
        lines_.push_back(std::move(line));
    }
    static DWORD WINAPI entry(void *p) {
        static_cast<Input *>(p)->run();
        return 0;
    }
    void run() noexcept {
        try {
            std::string buffer;
            std::array<char, 512> bytes{};
            while (!stop_) {
                DWORD n = 0;
                if (!ReadFile(GetStdHandle(STD_INPUT_HANDLE), bytes.data(), static_cast<DWORD>(bytes.size()), &n,
                              nullptr) ||
                    !n)
                    break;
                buffer.append(bytes.data(), n);
                std::lock_guard<std::mutex> lock(mutex_);
                std::size_t newline;
                while ((newline = buffer.find('\n')) != std::string::npos) {
                    enqueue(buffer.substr(0, newline));
                    buffer.erase(0, newline + 1);
                }
                // Allow a trailing CR while waiting for the LF of a CRLF line.
                if (buffer.size() > 8192 && !(buffer.size() == 8193 && buffer.back() == '\r'))
                    throw std::runtime_error("Command exceeds 8192 bytes");
                SetEvent(ready_.get());
            }
            std::lock_guard<std::mutex> lock(mutex_);
            if (!buffer.empty())
                enqueue(std::move(buffer));
            eof_ = true;
            SetEvent(ready_.get());
        } catch (const std::exception &e) {
            std::lock_guard<std::mutex> lock(mutex_);
            error_ = e.what();
            eof_ = true;
            SetEvent(ready_.get());
        }
    }

  public:
    Input() {
        thread_.reset(CreateThread(nullptr, 0, entry, this, 0, nullptr));
        if (!thread_)
            throw win_error("CreateThread stdin");
    }
    ~Input() {
        stop_ = true;
        while (WaitForSingleObject(thread_.get(), 20) == WAIT_TIMEOUT)
            CancelSynchronousIo(thread_.get());
    }
    HANDLE handle() const { return ready_.get(); }
    std::tuple<std::deque<std::string>, bool, std::string> take() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = std::move(lines_);
        lines_.clear();
        ResetEvent(ready_.get());
        return {std::move(result), eof_, std::exchange(error_, {})};
    }
};
Id number(const std::string &text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("Expected positive job ID");
    auto n = std::stoull(text);
    if (!n)
        throw std::runtime_error("Expected positive job ID");
    return n;
}
} // namespace
int main() try {
    SetConsoleCP(CP_UTF8);
    SetConsoleOutputCP(CP_UTF8);
    Options options;
    options.allow({"--session", "--trace"});
    Trace trace(trace_dir(options), "client");
    Channel channel(pipe_name(options.get("--session", "demo"), "client"), false);
    Input input;
    std::set<Id> pending;
    Id next_request = 1;
    bool quitting = false;
    std::cout << "Commands: submit route|time \"file.csv\", status [JOB_ID], cancel JOB_ID, shutdown, quit, help"
              << std::endl;
    trace.log("client_connected");
    while (true) {
        for (const auto &m : channel.pump()) {
            if (!pending.count(m.request))
                throw std::runtime_error("Uncorrelated Broker reply");
            trace.log("response_received", m, name(m.kind));
            std::cout << message_json(m) << std::endl;
            switch (m.kind) {
            case Kind::Accepted:
            case Kind::Progress:
                break; // The SUBMIT stays pending until its terminal reply.
            case Kind::ShutdownAck:
                quitting = true;
                pending.erase(m.request);
                break;
            case Kind::Completed:
            case Kind::Failed:
            case Kind::Cancelled:
            case Kind::CancelAck:
            case Kind::Rejected:
            case Kind::Status:
                pending.erase(m.request);
                break;
            default:
                throw std::runtime_error("Message not allowed from Broker");
            }
        }
        if (WaitForSingleObject(input.handle(), 0) == WAIT_OBJECT_0) {
            auto [lines, eof, error] = input.take();
            for (const auto &line : lines) {
                if (quitting || line.empty())
                    continue;
                try {
                    std::istringstream command(line);
                    std::string verb;
                    command >> verb;
                    Message m;
                    m.request = next_request;
                    if (verb == "help") {
                        std::cout << "submit route|time \"file.csv\" | status [JOB_ID] | cancel JOB_ID | shutdown "
                                     "(when idle) | quit (drain then exit)"
                                  << std::endl;
                        continue;
                    }
                    if (verb == "quit") {
                        quitting = true;
                        continue;
                    }
                    if (verb == "submit") {
                        // Windows backslashes are literal, not C++ quoted-string escapes.
                        std::string mode, path;
                        if (!(command >> mode >> std::quoted(path, '"', '\0')))
                            throw std::runtime_error("Expected analysis and a complete file path");
                        if (mode != "route" && mode != "time")
                            throw std::runtime_error("Analysis must be route or time");
                        if (path.empty() || !valid_utf8(path))
                            throw std::runtime_error("Expected UTF-8 file path");
                        m.kind = Kind::Submit;
                        m.analysis = mode == "route" ? Analysis::Route : Analysis::Time;
                        m.path =
                            utf8(std::filesystem::absolute(std::filesystem::u8path(path)).lexically_normal().wstring());
                    } else if (verb == "status") {
                        std::string id;
                        if (command >> id)
                            m.job = number(id);
                        else
                            command.clear();
                        m.kind = Kind::StatusRequest;
                    } else if (verb == "cancel") {
                        std::string id;
                        command >> id;
                        m.kind = Kind::Cancel;
                        m.job = number(id);
                    } else if (verb == "shutdown")
                        m.kind = Kind::Shutdown;
                    else
                        throw std::runtime_error("Unknown command; use help");
                    std::string extra;
                    if (command >> extra)
                        throw std::runtime_error("Unexpected trailing argument");
                    trace.log("command_started", m, name(m.kind));
                    channel.send(m);
                    pending.insert(next_request++);
                    trace.log("command_sent", m, name(m.kind));
                    std::cout << "{\"event\":\"command_sent\",\"request_id\":" << m.request
                              << ",\"type\":" << quote_json(name(m.kind)) << "}" << std::endl;
                } catch (const std::exception &e) {
                    std::cout << "command error: " << e.what() << std::endl;
                }
            }
            if (!error.empty())
                std::cout << "input error: " << error << std::endl;
            if (eof)
                quitting = true; // Drain requests already sent, even after input failure.
        }
        if (quitting && pending.empty())
            channel.stop_receiving();
        // A received final ACK proves its correlated request reached Broker.
        // Expected peer close may race local write-completion collection.
        if (quitting && pending.empty() && (channel.drained() || channel.closed()))
            break;
        if (channel.closed())
            throw std::runtime_error("Broker disconnected with outstanding requests");
        std::vector<HANDLE> events;
        channel.append_events(events);
        if (!quitting)
            events.push_back(input.handle());
        wait_events(events);
    }
    trace.log("client_stopped");
    return 0;
} catch (const std::exception &e) {
    std::cerr << "client: " << e.what() << std::endl;
    return 1;
}
