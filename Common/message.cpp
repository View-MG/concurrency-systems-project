#include "message.hpp"
#include "analyzer.hpp"
#include "platform.hpp"
#include <sstream>

namespace lga {
const char *name(Kind k) {
    switch (k) {
    case Kind::Ready:
        return "READY";
    case Kind::Submit:
        return "SUBMIT";
    case Kind::Accepted:
        return "ACCEPTED";
    case Kind::Rejected:
        return "REJECTED";
    case Kind::Task:
        return "TASK";
    case Kind::Result:
        return "RESULT";
    case Kind::StatusRequest:
        return "STATUS_REQUEST";
    case Kind::Status:
        return "STATUS";
    case Kind::Completed:
        return "COMPLETED";
    case Kind::Failed:
        return "FAILED";
    case Kind::Shutdown:
        return "SHUTDOWN";
    case Kind::ShutdownAck:
        return "SHUTDOWN_ACK";
    case Kind::Cancel:
        return "CANCEL";
    case Kind::CancelAck:
        return "CANCEL_ACK";
    case Kind::Cancelled:
        return "CANCELLED";
    case Kind::Progress:
        return "PROGRESS";
    }
    throw std::runtime_error("Unknown message kind");
}
namespace {
struct Writer {
    std::vector<std::uint8_t> b;
    void num(std::uint64_t x, unsigned n = 8) {
        for (unsigned i = 0; i < n; ++i)
            b.push_back(static_cast<std::uint8_t>(x >> (8 * i)));
    }
    void str(const std::string &s) {
        if (s.size() > max_payload || !valid_utf8(s) || s.find('\0') != std::string::npos)
            throw std::runtime_error("Invalid wire string");
        num(s.size(), 4);
        b.insert(b.end(), s.begin(), s.end());
    }
    void summary(const Summary &s) {
        num(s.valid);
        num(s.malformed);
        num(s.groups.size(), 4);
        if (s.groups.size() > max_groups)
            throw std::runtime_error("Too many keys");
        for (const auto &[key, c] : s.groups) {
            str(key);
            num(c.errors);
            num(c.errors4xx);
            num(c.errors5xx);
        }
    }
};
struct Reader {
    const std::uint8_t *p;
    std::size_t left;
    std::uint64_t num(unsigned n = 8) {
        if (left < n)
            throw std::runtime_error("Truncated payload");
        std::uint64_t x = 0;
        for (unsigned i = 0; i < n; ++i)
            x |= static_cast<std::uint64_t>(p[i]) << (8 * i);
        p += n;
        left -= n;
        return x;
    }
    std::string str() {
        auto n = num(4);
        if (n > left)
            throw std::runtime_error("Truncated string");
        std::string s(reinterpret_cast<const char *>(p), static_cast<std::size_t>(n));
        p += n;
        left -= n;
        if (!valid_utf8(s) || s.find('\0') != std::string::npos)
            throw std::runtime_error("Invalid wire string");
        return s;
    }
    Summary summary() {
        Summary s;
        s.valid = num();
        s.malformed = num();
        auto n = num(4);
        if (n > max_groups)
            throw std::runtime_error("Too many keys");
        std::uint64_t remaining_errors = s.valid;
        for (std::uint64_t i = 0; i < n; ++i) {
            auto key = str();
            Counts c;
            c.errors = num();
            c.errors4xx = num();
            c.errors5xx = num();
            if (key.empty() || key.size() > 256 || c.errors > remaining_errors || c.errors4xx > c.errors ||
                c.errors5xx != c.errors - c.errors4xx || !s.groups.emplace(key, c).second)
                throw std::runtime_error("Invalid count or duplicate key");
            remaining_errors -= c.errors;
        }
        return s;
    }
};
Analysis analysis(Reader &r) {
    auto n = r.num(1);
    if (n < 1 || n > 2)
        throw std::runtime_error("Invalid analysis");
    return static_cast<Analysis>(n);
}
Message decode(Kind k, const std::uint8_t *p, std::size_t n) {
    Reader r{p, n};
    Message m;
    m.kind = k;
    switch (k) {
    case Kind::Ready:
        m.worker = r.num();
        break;
    case Kind::Submit:
        m.request = r.num();
        m.analysis = analysis(r);
        m.path = r.str();
        break;
    case Kind::Accepted:
        m.request = r.num();
        m.job = r.num();
        m.total_chunks = r.num();
        break;
    case Kind::Rejected:
        m.request = r.num();
        m.reason = r.str();
        break;
    case Kind::Task:
        m.request = r.num();
        m.job = r.num();
        m.chunk = r.num();
        m.worker = r.num();
        m.analysis = analysis(r);
        m.path = r.str();
        m.begin = r.num();
        m.end = r.num();
        m.file_size = r.num();
        m.file_mtime = r.num();
        m.file_volume = r.num();
        m.file_id = r.num();
        break;
    case Kind::Result:
    case Kind::Completed:
    case Kind::Failed:
        m.request = r.num();
        m.job = r.num();
        m.chunk = r.num();
        m.worker = r.num();
        {
            auto v = r.num(1);
            if (v > 1)
                throw std::runtime_error("Invalid success flag");
            m.success = v != 0;
        }
        m.reason = r.str();
        m.duration_us = r.num();
        m.summary = r.summary();
        break;
    case Kind::StatusRequest:
    case Kind::Cancel:
        m.request = r.num();
        m.job = r.num();
        break;
    case Kind::Status:
    case Kind::Progress:
    case Kind::CancelAck:
    case Kind::Cancelled:
        m.request = r.num();
        m.job = r.num();
        {
            auto v = r.num(1);
            if (v < 1 || v > 6)
                throw std::runtime_error("Invalid job state");
            m.state = static_cast<JobState>(v);
        }
        {
            auto v = r.num(1);
            if (v < 1 || v > 3)
                throw std::runtime_error("Invalid worker state");
            m.worker_state = static_cast<WorkerState>(v);
        }
        m.total_chunks = r.num();
        m.completed_chunks = r.num();
        m.in_flight = r.num();
        m.pending_chunks = r.num();
        m.monitor = r.str();
        if (m.total_chunks > max_chunks || m.completed_chunks > m.total_chunks ||
            m.in_flight > m.total_chunks - m.completed_chunks ||
            m.pending_chunks > m.total_chunks - m.completed_chunks - m.in_flight)
            throw std::runtime_error("Invalid progress counters");
        break;
    case Kind::Shutdown:
    case Kind::ShutdownAck:
        m.request = r.num();
        break;
    default:
        throw std::runtime_error("Unknown kind");
    }
    if (r.left)
        throw std::runtime_error("Trailing payload bytes");
    return m;
}
} // namespace
std::vector<std::uint8_t> encode(const Message &m) {
    Writer p;
    switch (m.kind) {
    case Kind::Ready:
        p.num(m.worker);
        break;
    case Kind::Submit:
        p.num(m.request);
        p.num(static_cast<unsigned>(m.analysis), 1);
        p.str(m.path);
        break;
    case Kind::Accepted:
        p.num(m.request);
        p.num(m.job);
        p.num(m.total_chunks);
        break;
    case Kind::Rejected:
        p.num(m.request);
        p.str(m.reason);
        break;
    case Kind::Task:
        p.num(m.request);
        p.num(m.job);
        p.num(m.chunk);
        p.num(m.worker);
        p.num(static_cast<unsigned>(m.analysis), 1);
        p.str(m.path);
        p.num(m.begin);
        p.num(m.end);
        p.num(m.file_size);
        p.num(m.file_mtime);
        p.num(m.file_volume);
        p.num(m.file_id);
        break;
    case Kind::Result:
    case Kind::Completed:
    case Kind::Failed:
        p.num(m.request);
        p.num(m.job);
        p.num(m.chunk);
        p.num(m.worker);
        p.num(m.success, 1);
        p.str(m.reason);
        p.num(m.duration_us);
        p.summary(m.summary);
        break;
    case Kind::StatusRequest:
    case Kind::Cancel:
        p.num(m.request);
        p.num(m.job);
        break;
    case Kind::Status:
    case Kind::Progress:
    case Kind::CancelAck:
    case Kind::Cancelled:
        p.num(m.request);
        p.num(m.job);
        p.num(static_cast<unsigned>(m.state), 1);
        p.num(static_cast<unsigned>(m.worker_state), 1);
        p.num(m.total_chunks);
        p.num(m.completed_chunks);
        p.num(m.in_flight);
        p.num(m.pending_chunks);
        p.str(m.monitor);
        break;
    case Kind::Shutdown:
    case Kind::ShutdownAck:
        p.num(m.request);
        break;
    default:
        throw std::runtime_error("Unknown kind");
    }
    if (p.b.size() > max_payload)
        throw std::runtime_error("Payload exceeds 2 MiB");
    Writer frame;
    frame.num(0x3141474c, 4);
    frame.num(2, 2);
    frame.num(static_cast<unsigned>(m.kind), 2);
    frame.num(p.b.size(), 4);
    frame.b.insert(frame.b.end(), p.b.begin(), p.b.end());
    return frame.b;
}
std::vector<Message> Decoder::feed(const std::uint8_t *data, std::size_t size) {
    if (size > max_payload + 12 || buffered_.size() + size > 2 * (max_payload + 12))
        throw std::runtime_error("Receive buffer limit");
    buffered_.insert(buffered_.end(), data, data + size);
    std::vector<Message> messages;
    std::size_t used = 0;
    while (buffered_.size() - used >= 12) {
        Reader h{buffered_.data() + used, 12};
        if (h.num(4) != 0x3141474c || h.num(2) != 2)
            throw std::runtime_error("Invalid magic/version");
        auto kind = h.num(2), length = h.num(4);
        if (kind < 1 || kind > 16 || length > max_payload)
            throw std::runtime_error("Invalid kind/length");
        if (buffered_.size() - used < length + 12)
            break;
        messages.push_back(
            decode(static_cast<Kind>(kind), buffered_.data() + used + 12, static_cast<std::size_t>(length)));
        used += static_cast<std::size_t>(length) + 12;
    }
    buffered_.erase(buffered_.begin(), buffered_.begin() + static_cast<std::ptrdiff_t>(used));
    return messages;
}
std::string message_json(const Message &m) {
    std::ostringstream s;
    s << "{\"type\":" << quote_json(name(m.kind)) << ",\"request_id\":" << m.request << ",\"job_id\":" << m.job
      << ",\"chunk_id\":" << m.chunk << ",\"worker_id\":" << m.worker;
    if (m.kind == Kind::Accepted)
        s << ",\"total_chunks\":" << m.total_chunks;
    if (m.kind == Kind::Status || m.kind == Kind::Progress || m.kind == Kind::CancelAck || m.kind == Kind::Cancelled)
        s << ",\"job_state\":" << quote_json(name(m.state)) << ",\"worker_state\":" << quote_json(name(m.worker_state))
          << ",\"total_chunks\":" << m.total_chunks << ",\"completed_chunks\":" << m.completed_chunks
          << ",\"in_flight\":" << m.in_flight << ",\"pending_chunks\":" << m.pending_chunks
          << ",\"monitor\":" << (m.monitor.empty() ? "{}" : m.monitor);
    if (m.kind == Kind::Rejected || m.kind == Kind::Failed)
        s << ",\"reason\":" << quote_json(m.reason);
    if (m.kind == Kind::Completed)
        s << ",\"worker_compute_us\":" << m.duration_us << ",\"summary\":" << summary_json(m.summary);
    return s.str() + "}";
}
} // namespace lga
