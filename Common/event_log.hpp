#pragma once
#include "message.hpp"
#include "platform.hpp"
#include <fstream>
#include <cstdio>
namespace lga {
class Trace {
    std::ofstream out_;
    std::string component_;
    std::uint64_t frequency_ = frequency();

  public:
    Trace(const std::filesystem::path &directory, const std::string &component) : component_(component) {
        std::filesystem::create_directories(directory);
        out_.open(directory / (component + ".jsonl"), std::ios::trunc);
        if (!out_)
            throw std::runtime_error("Cannot open trace file");
    }
    void log(const std::string &event_name, const Message &m = {}, const std::string &detail = "") {
        // TASK/RESULT carry no Broker job state. Do not print the Message
        // default (Queued) as if it described a running or cancelling job.
        const auto state = m.kind == Kind::Completed ? JobState::Completed
                           : m.kind == Kind::Failed  ? JobState::Failed
                                                     : m.state;
        const bool has_state = event_name.rfind("job_", 0) == 0 || event_name == "chunk_merged" ||
                               m.kind == Kind::Accepted || m.kind == Kind::Completed || m.kind == Kind::Failed ||
                               m.kind == Kind::Status || m.kind == Kind::Progress || m.kind == Kind::CancelAck ||
                               m.kind == Kind::Cancelled;
        SYSTEMTIME wall{};
        GetSystemTime(&wall);
        char utc[32]{};
        std::snprintf(utc, sizeof(utc), "%04u-%02u-%02uT%02u:%02u:%02u.%03uZ", wall.wYear, wall.wMonth, wall.wDay,
                      wall.wHour, wall.wMinute, wall.wSecond, wall.wMilliseconds);
        out_ << "{\"qpc_ticks\":" << ticks() << ",\"qpc_frequency\":" << frequency_
             << ",\"pid\":" << GetCurrentProcessId() << ",\"wall_utc\":" << quote_json(utc)
             << ",\"component\":" << quote_json(component_) << ",\"event\":" << quote_json(event_name)
             << ",\"request_id\":" << m.request << ",\"job_id\":" << m.job << ",\"chunk_id\":" << m.chunk
             << ",\"worker_id\":" << m.worker << ",\"begin\":" << m.begin << ",\"end\":" << m.end
             << ",\"job_state\":" << (has_state ? quote_json(name(state)) : "null")
             << ",\"total_chunks\":" << m.total_chunks << ",\"completed_chunks\":" << m.completed_chunks
             << ",\"in_flight\":" << m.in_flight << ",\"pending_chunks\":" << m.pending_chunks
             << ",\"duration_us\":" << m.duration_us << ",\"detail\":" << quote_json(detail) << "}\n";
        out_.flush();
        if (!out_)
            throw std::runtime_error("Trace write failed");
    }
};
inline std::filesystem::path trace_dir(const Options &o) {
    return std::filesystem::u8path(o.get("--trace", "build/traces/" + o.get("--session", "demo")));
}
} // namespace lga
