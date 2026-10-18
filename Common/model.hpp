#pragma once
#include <cstdint>
#include <map>
#include <string>

namespace lga {
using Id = std::uint64_t;
enum class Analysis : std::uint8_t { Route = 1, Time = 2 };
enum class JobState : std::uint8_t { Queued = 1, Active, Cancelling, Cancelled, Completed, Failed };
enum class WorkerState : std::uint8_t { Ready = 1, Busy, Disconnected };
inline const char *name(JobState s) {
    switch (s) {
    case JobState::Queued:
        return "Queued";
    case JobState::Active:
        return "Active";
    case JobState::Cancelling:
        return "Cancelling";
    case JobState::Cancelled:
        return "Cancelled";
    case JobState::Completed:
        return "Completed";
    case JobState::Failed:
        return "Failed";
    }
    return "Invalid";
}
inline const char *name(WorkerState s) {
    switch (s) {
    case WorkerState::Ready:
        return "Ready";
    case WorkerState::Busy:
        return "Busy";
    case WorkerState::Disconnected:
        return "Disconnected";
    }
    return "Invalid";
}
inline const char *name(Analysis a) {
    return a == Analysis::Route ? "route" : "time";
}
struct Counts {
    std::uint64_t errors = 0, errors4xx = 0, errors5xx = 0;
};
struct Summary {
    std::uint64_t valid = 0, malformed = 0;
    std::map<std::string, Counts> groups;
};
constexpr std::uint64_t max_file_bytes = 8ULL * 1024 * 1024 * 1024;
constexpr std::size_t max_groups = 4096;
constexpr std::uint32_t max_payload = 2 * 1024 * 1024;
constexpr std::size_t default_capacity = 32;
constexpr std::size_t max_chunks = 65536;
constexpr std::size_t max_workers = 8;
} // namespace lga
