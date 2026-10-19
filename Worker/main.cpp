#include "analyzer.hpp"
#include "event_log.hpp"
#include "ipc.hpp"
#include <iostream>
using namespace lga;
int main() try {
    Options options;
    options.allow({"--session", "--trace", "--delay-ms", "--id"});
    const auto worker_id = options.number("--id", 1, 1, max_workers);
    const auto delay = options.number("--delay-ms", 0, 0, 10000);
    const auto role = worker_id == 1 ? std::string("worker") : "worker" + std::to_string(worker_id);
    Trace trace(trace_dir(options), role);
    // [Flow 2] Worker ต่อ pipe ของตัวเองแล้วส่ง READY → Broker ตั้งสถานะ Worker เป็น Ready
    Channel channel(pipe_name(options.get("--session", "demo"), role.c_str()), false);
    Message ready;
    ready.kind = Kind::Ready;
    ready.worker = worker_id;
    channel.send(ready);
    trace.log("worker_ready", ready);
    bool stopping = false;
    while (true) {
        for (const auto &m : channel.pump()) {
            // [Flow 8] รับ TASK: ตรวจ identity/range → analyze_chunk (Common/analyzer.cpp) → ส่ง RESULT กลับ Broker
            if (m.kind == Kind::Task && !stopping) {
                if (!m.request || !m.job || !m.chunk || m.chunk > max_chunks || m.worker != worker_id ||
                    m.begin > m.end || m.end > m.file_size || m.file_size > max_file_bytes)
                    throw std::runtime_error("Invalid task identity/range");
                trace.log("task_received", m);
                Message result = m;
                result.kind = Kind::Result;
                try {
                    if (delay) {
                        trace.log("test_delay_started", m, std::to_string(delay));
                        Sleep(static_cast<DWORD>(delay));
                    }
                    trace.log("compute_started", m);
                    const auto start = ticks();
                    result.summary = analyze_chunk(std::filesystem::u8path(m.path), m.analysis, m.begin, m.end,
                                                   {m.file_size, m.file_mtime, m.file_volume, m.file_id});
                    result.duration_us = elapsed_us(start);
                    trace.log("compute_finished", result, std::to_string(result.duration_us));
                } catch (const std::exception &e) {
                    result.success = false;
                    result.reason = e.what();
                    trace.log("compute_failed", result, e.what());
                }
                channel.send(result);
                trace.log("result_queued", result);
            } else if (m.kind == Kind::Shutdown) {
                stopping = true;
                channel.stop_receiving();
                Message ack = m;
                ack.kind = Kind::ShutdownAck;
                channel.send(ack);
                trace.log("shutdown_received", m);
            } else
                throw std::runtime_error("Message not allowed from Broker");
        }
        if (stopping && channel.drained())
            break;
        if (channel.closed())
            throw std::runtime_error("Broker disconnected");
        std::vector<HANDLE> events;
        channel.append_events(events);
        wait_events(events);
    }
    trace.log("worker_stopped");
    return 0;
} catch (const std::exception &e) {
    std::cerr << "worker: " << e.what() << std::endl;
    return 1;
}
