#pragma once
#include "model.hpp"
#include <vector>
namespace lga {
enum class Kind : std::uint16_t {
    Ready = 1,
    Submit,
    Accepted,
    Rejected,
    Task,
    Result,
    StatusRequest,
    Status,
    Completed,
    Failed,
    Shutdown,
    ShutdownAck,
    Cancel,
    CancelAck,
    Cancelled,
    Progress
};
const char *name(Kind kind);
struct Message {
    Kind kind = Kind::Ready;
    Id request = 0, job = 0, chunk = 0, worker = 0;
    Analysis analysis = Analysis::Route;
    JobState state = JobState::Queued;
    WorkerState worker_state = WorkerState::Disconnected;
    std::string path, reason, monitor = "{}";
    std::uint64_t begin = 0, end = 0, duration_us = 0;
    std::uint64_t file_size = 0, file_mtime = 0, file_volume = 0, file_id = 0;
    std::uint64_t total_chunks = 0, completed_chunks = 0, in_flight = 0, pending_chunks = 0;
    bool success = true;
    Summary summary;
};
std::vector<std::uint8_t> encode(const Message &message);
class Decoder {
    std::vector<std::uint8_t> buffered_;

  public:
    std::vector<Message> feed(const std::uint8_t *data, std::size_t size);
};
std::string message_json(const Message &message);
} // namespace lga
