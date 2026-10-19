#include "chunks.hpp"
#include "analyzer.hpp"
#include "event_log.hpp"
#include "ipc.hpp"
#include <algorithm>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <limits>
#include <sstream>
using namespace lga;
namespace {
HANDLE stop_event = nullptr;
BOOL WINAPI interrupt(DWORD type) {
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
        if (stop_event)
            SetEvent(stop_event);
        return TRUE;
    }
    return FALSE;
}
bool terminal(JobState s) {
    return s == JobState::Completed || s == JobState::Cancelled || s == JobState::Failed;
}
struct Job {
    Message base;
    JobState state = JobState::Queued;
    ChunkIndex index;
    std::unique_ptr<LockedInput> input;
    std::vector<bool> merged;
    std::size_t next = 0, completed = 0, in_flight = 0;
    Summary summary;
    std::uint64_t accepted = 0, first = 0, finished = 0, compute_us = 0, last_progress = 0;
    bool settled() const { return terminal(state) && !in_flight; }
    std::size_t pending_chunks() const {
        return terminal(state) || state == JobState::Cancelling || next >= index.chunks.size()
                   ? 0
                   : index.chunks.size() - next;
    }
};
struct Worker {
    Id id;
    Channel channel;
    WorkerState state = WorkerState::Disconnected;
    Message assignment;
    bool seen = false, shutdown_sent = false, stopped = false;
    Worker(Id n, const std::string &session)
        : id(n),
          channel(pipe_name(session, (n == 1 ? std::string("worker") : "worker" + std::to_string(n)).c_str()), true) {}
};
} // namespace
int main() try {
    Options options;
    options.allow({"--session", "--trace", "--workers", "--policy", "--capacity", "--io-mode"});
    auto worker_count = options.number("--workers", 3, 1, max_workers),
         capacity = options.number("--capacity", default_capacity, 1, 128);
    auto policy = options.get("--policy", "fifo"), io_mode = options.get("--io-mode", "async"),
         session = options.get("--session", "demo");
    if (policy != "fifo" && policy != "rr")
        throw std::runtime_error("policy must be fifo or rr");
    if (io_mode != "async" && io_mode != "sequential-wait")
        throw std::runtime_error("io-mode must be async or sequential-wait");
    const auto progress_interval = frequency() / 20;
    Trace trace(trace_dir(options), "broker");
    Channel client(pipe_name(session, "client"), true);
    std::vector<std::unique_ptr<Worker>> workers;
    for (std::size_t i = 1; i <= worker_count; ++i)
        workers.push_back(std::make_unique<Worker>(i, session));
    Handle stop = event();
    stop_event = stop.get();
    SetConsoleCtrlHandler(interrupt, TRUE);
    std::map<Id, Job> jobs;
    std::deque<Id> ready;
    Id next_job = 1, last_request = 0, shutdown_request = 0;
    bool stopping = false, client_lost = false, disconnect_pending = false, shutdown_reply_sent = false;
    ULONGLONG shutdown_handshake_deadline = 0;
    trace.log("broker_started", {},
              "policy=" + policy + ";io_mode=" + io_mode + ";workers=" + std::to_string(worker_count));
    std::cout << "BROKER_READY" << std::endl;
    auto reply = [&](const Message &m) {
        if (client_lost || disconnect_pending || !client.connected())
            return;
        try {
            client.send(m);
            trace.log("response_queued", m, name(m.kind));
        } catch (const std::exception &e) {
            // Backpressure never silently drops a terminal response: disconnect,
            // cancel owned work, and settle every actual assignment instead.
            trace.log("client_delivery_failed", m, e.what());
            client.close();
            disconnect_pending = true;
        }
    };
    auto reject = [&](Id request, const std::string &why) {
        Message m;
        m.kind = Kind::Rejected;
        m.request = request;
        m.reason = why;
        reply(m);
        trace.log("request_rejected", m, why);
    };
    auto fill_progress = [&](Message &m, const Job &j) {
        m.state = j.state;
        m.total_chunks = j.index.chunks.size();
        m.completed_chunks = j.completed;
        m.in_flight = j.in_flight;
        m.pending_chunks = j.pending_chunks();
        m.worker_state = WorkerState::Disconnected;
        for (const auto &w : workers)
            if (w->state == WorkerState::Busy && w->assignment.job == j.base.job)
                m.worker_state = WorkerState::Busy;
        if (m.worker_state != WorkerState::Busy)
            for (const auto &w : workers)
                if (w->state == WorkerState::Ready) {
                    m.worker_state = WorkerState::Ready;
                    break;
                }
    };
    auto snapshot = [&]() {
        std::ostringstream out;
        out << "{\"policy\":" << quote_json(policy) << ",\"io_mode\":" << quote_json(io_mode)
            << ",\"capacity\":" << capacity << ",\"ready_jobs\":[";
        bool comma = false;
        const auto separate = [&] { out << (std::exchange(comma, true) ? "," : ""); };
        for (Id id : ready) {
            separate();
            out << id;
        }
        out << "],\"jobs\":[";
        comma = false;
        for (const auto &[id, j] : jobs) {
            separate();
            Message m;
            fill_progress(m, j);
            out << "{\"job_id\":" << id << ",\"request_id\":" << j.base.request
                << ",\"path\":" << quote_json(j.base.path) << ",\"analysis\":" << quote_json(name(j.base.analysis))
                << ",\"state\":" << quote_json(name(j.state)) << ",\"total_chunks\":" << m.total_chunks
                << ",\"completed_chunks\":" << m.completed_chunks << ",\"in_flight\":" << m.in_flight
                << ",\"pending_chunks\":" << m.pending_chunks << ",\"valid_records\":" << j.summary.valid
                << ",\"malformed_records\":" << j.summary.malformed << ",\"accepted_ticks\":" << j.accepted
                << ",\"first_assignment_ticks\":" << j.first << ",\"terminal_ticks\":" << j.finished << '}';
        }
        out << "],\"workers\":[";
        comma = false;
        for (const auto &w : workers) {
            separate();
            out << "{\"worker_id\":" << w->id << ",\"state\":" << quote_json(name(w->state))
                << ",\"job_id\":" << (w->state == WorkerState::Busy ? w->assignment.job : 0)
                << ",\"chunk_id\":" << (w->state == WorkerState::Busy ? w->assignment.chunk : 0) << '}';
        }
        std::uint64_t messages = client.sent_messages() + client.received_messages(),
                      bytes = client.sent_bytes() + client.received_bytes();
        for (const auto &w : workers) {
            messages += w->channel.sent_messages() + w->channel.received_messages();
            bytes += w->channel.sent_bytes() + w->channel.received_bytes();
        }
        out << "],\"qpc_ticks\":" << ticks() << ",\"qpc_frequency\":" << frequency() << ",\"ipc_messages\":" << messages
            << ",\"ipc_bytes\":" << bytes << '}';
        return out.str();
    };
    auto remove_ready = [&](Id id) { ready.erase(std::remove(ready.begin(), ready.end(), id), ready.end()); };
    auto notify_terminal = [&](Job &j, Kind kind, const std::string &why) {
        j.finished = ticks();
        remove_ready(j.base.job);
        auto m = j.base;
        m.kind = kind;
        m.reason = why;
        m.success = kind == Kind::Completed;
        m.summary = j.summary;
        m.duration_us = j.compute_us;
        fill_progress(m, j);
        reply(m);
        trace.log(kind == Kind::Completed   ? "job_completed"
                  : kind == Kind::Cancelled ? "job_cancelled"
                                            : "job_failed",
                  m, why);
    };
    auto settle = [&](Job &j) {
        if (j.state == JobState::Cancelling && !j.in_flight) {
            j.state = JobState::Cancelled;
            notify_terminal(j, Kind::Cancelled, "Cancelled at chunk boundary");
        }
        if (j.settled())
            j.input.reset();
    };
    auto fail = [&](Job &j, const std::string &why) {
        if (terminal(j.state) || j.state == JobState::Cancelling)
            return;
        j.state = JobState::Failed;
        notify_terminal(j, Kind::Failed, why);
    };
    auto cancel = [&](Job &j) {
        if (terminal(j.state) || j.state == JobState::Cancelling)
            return;
        remove_ready(j.base.job);
        j.state = JobState::Cancelling;
        auto m = j.base;
        fill_progress(m, j);
        trace.log("job_cancelling", m, "Stop new assignments; settle running chunks");
    };
    auto lose_worker = [&](Worker &w, const std::string &why) {
        auto m = w.assignment;
        m.worker = w.id;
        trace.log("worker_disconnected", m, why);
        w.channel.close();
        const bool busy = w.state == WorkerState::Busy;
        w.state = WorkerState::Disconnected;
        w.seen = true;
        w.stopped = true;
        w.assignment = {};
        if (busy) {
            auto &j = jobs.at(m.job);
            --j.in_flight;
            fail(j, why);
            settle(j);
        }
        if (std::all_of(workers.begin(), workers.end(), [](const auto &peer) { return peer->channel.closed(); }))
            for (auto &[id, j] : jobs) {
                (void)id;
                fail(j, "All Workers disconnected");
                settle(j);
            }
    };
    auto settle_disconnect = [&]() {
        if (!disconnect_pending || client_lost)
            return;
        client_lost = true;
        stopping = true;
        trace.log("client_disconnected");
        client.close();
        for (auto &[id, j] : jobs) {
            (void)id;
            cancel(j);
            settle(j);
        }
    };
    while (true) {
        // 1. Handle Client commands without waiting for any Worker computation.
        if (WaitForSingleObject(stop.get(), 0) == WAIT_OBJECT_0) {
            ResetEvent(stop.get());
            stopping = true;
            trace.log("stop_requested", {}, "Drain admitted work");
        }
        if (!client_lost && !disconnect_pending)
            try {
                for (const auto &m : client.pump()) {
                    if (!m.request || m.request <= last_request)
                        throw std::runtime_error("request_id must increase strictly");
                    last_request = m.request;
                    trace.log("client_message_received", m, name(m.kind));
                    // [Flow 6] รับ SUBMIT: ตรวจเพดาน ล็อกไฟล์ load_index สร้าง Job เข้า ready queue แล้วตอบ ACCEPTED
                    if (m.kind == Kind::Submit) {
                        if (stopping) {
                            reject(m.request, "Broker is stopping");
                            continue;
                        }
                        auto count = std::count_if(jobs.begin(), jobs.end(),
                                                   [](const auto &entry) { return !terminal(entry.second.state); });
                        if (static_cast<std::size_t>(count) >= capacity) {
                            reject(m.request, "Active job capacity reached");
                            continue;
                        }
                        if (std::all_of(workers.begin(), workers.end(),
                                        [](const auto &w) { return w->channel.closed(); })) {
                            reject(m.request, "Workers unavailable");
                            continue;
                        }
                        try {
                            auto path = std::filesystem::absolute(std::filesystem::u8path(m.path)).lexically_normal();
                            if (!std::filesystem::is_regular_file(path))
                                throw std::runtime_error("Input is not a regular file");
                            Job j;
                            j.base = m;
                            j.base.path = utf8(path.wstring());
                            j.base.job = next_job;
                            j.input = std::make_unique<LockedInput>(path);
                            auto sidecar = index_path(path);
                            if (std::filesystem::exists(sidecar))
                                j.index = load_index(path, *j.input);
                            else {
                                if (j.input->stamp().size > 8 * 1024 * 1024)
                                    throw std::runtime_error("Run prepare --file FILE --chunk-bytes N before Submit");
                                j.index.stamp = j.input->stamp();
                                j.index.target_bytes = j.input->stamp().size;
                                j.index.chunks.push_back({0, j.input->stamp().size});
                            }
                            const auto &stamp = j.index.stamp;
                            j.base.file_size = stamp.size;
                            j.base.file_mtime = stamp.mtime;
                            j.base.file_volume = stamp.volume;
                            j.base.file_id = stamp.file_id;
                            j.merged.resize(j.index.chunks.size());
                            j.accepted = ticks();
                            if (jobs.size() >= 256) {
                                auto old = std::find_if(jobs.begin(), jobs.end(),
                                                        [](const auto &entry) { return entry.second.settled(); });
                                if (old == jobs.end())
                                    throw std::runtime_error("Job history capacity reached");
                                jobs.erase(old);
                            }
                            jobs.emplace(next_job, std::move(j));
                            ready.push_back(next_job);
                            auto response = jobs.at(next_job).base;
                            response.kind = Kind::Accepted;
                            fill_progress(response, jobs.at(next_job));
                            trace.log("job_accepted", response);
                            reply(response);
                            ++next_job;
                            bool free = std::any_of(workers.begin(), workers.end(),
                                                    [](const auto &w) { return w->state == WorkerState::Ready; });
                            trace.log(free ? "job_waiting_scheduling_turn" : "job_waiting_free_worker", response,
                                      policy);
                        } catch (const std::exception &e) {
                            reject(m.request, e.what());
                        }
                    } else if (m.kind == Kind::StatusRequest) {
                        auto response = m;
                        response.kind = Kind::Status;
                        if (m.job) {
                            auto it = jobs.find(m.job);
                            if (it == jobs.end()) {
                                reject(m.request, "Unknown or evicted job_id");
                                continue;
                            }
                            fill_progress(response, it->second);
                        }
                        response.monitor = snapshot();
                        reply(response);
                        trace.log("status_replied", response, name(response.state));
                    } else if (m.kind == Kind::Cancel) {
                        auto it = jobs.find(m.job);
                        if (it == jobs.end()) {
                            reject(m.request, "Unknown or evicted job_id");
                            continue;
                        }
                        if (terminal(it->second.state)) {
                            reject(m.request, "Job is already terminal");
                            continue;
                        }
                        cancel(it->second);
                        auto ack = m;
                        ack.kind = Kind::CancelAck;
                        fill_progress(ack, it->second);
                        reply(ack);
                        settle(it->second);
                    } else if (m.kind == Kind::Shutdown) {
                        if (stopping) {
                            reject(m.request, "Broker is already stopping");
                            continue;
                        }
                        bool outstanding = std::any_of(jobs.begin(), jobs.end(),
                                                       [](const auto &entry) { return !entry.second.settled(); });
                        if (outstanding) {
                            reject(m.request, "Jobs still outstanding; cancel or wait before shutdown");
                            continue;
                        }
                        stopping = true;
                        shutdown_request = m.request;
                        trace.log("shutdown_requested", m);
                    } else
                        throw std::runtime_error("Message not allowed from Client");
                }
                if (client.closed())
                    disconnect_pending = true;
            } catch (const std::exception &e) {
                trace.log("client_protocol_failure", {}, e.what());
                client.close();
                disconnect_pending = true;
            }
        settle_disconnect();
        // 2. Validate results before merging; settle the actual assignment even
        //    when its Job has already failed or is being cancelled.
        for (auto &owned : workers) {
            auto &w = *owned;
            if (w.channel.closed())
                continue;
            try {
                for (const auto &m : w.channel.pump()) {
                    if (m.kind == Kind::Ready) {
                        if (w.seen || m.worker != w.id)
                            throw std::runtime_error("Invalid READY identity/state");
                        w.seen = true;
                        w.state = WorkerState::Ready;
                        trace.log("worker_ready", m);
                    } else if (m.kind == Kind::Result) {
                        // [Flow 9] ตรวจ RESULT ตรง assignment → merge_summary → ส่ง PROGRESS; ครบทุก chunk ส่ง COMPLETED
                        trace.log("result_received", m);
                        if (w.state != WorkerState::Busy || m.worker != w.id || m.request != w.assignment.request ||
                            m.job != w.assignment.job || m.chunk != w.assignment.chunk)
                            throw std::runtime_error("Result does not match current assignment");
                        auto &j = jobs.at(m.job);
                        auto index = static_cast<std::size_t>(m.chunk - 1);
                        if (index >= j.merged.size() || j.merged[index])
                            throw std::runtime_error("Duplicate chunk result");
                        --j.in_flight;
                        w.state = WorkerState::Ready;
                        w.assignment = {};
                        if (j.state == JobState::Active) {
                            if (!m.success)
                                fail(j, m.reason);
                            else
                                try {
                                    auto bytes = j.index.chunks[index].end - j.index.chunks[index].begin;
                                    if (m.summary.valid > bytes || m.summary.malformed > bytes - m.summary.valid)
                                        throw std::runtime_error("Impossible chunk record counts");
                                    if (m.duration_us > std::numeric_limits<std::uint64_t>::max() - j.compute_us)
                                        throw std::runtime_error("Compute duration overflow");
                                    merge_summary(j.summary, m.summary);
                                    j.merged[index] = true;
                                    ++j.completed;
                                    j.compute_us += m.duration_us;
                                    auto progress = j.base;
                                    progress.kind = Kind::Progress;
                                    progress.chunk = m.chunk;
                                    progress.worker = m.worker;
                                    fill_progress(progress, j);
                                    trace.log("chunk_merged", progress);
                                    if (j.completed == j.index.chunks.size()) {
                                        j.state = JobState::Completed;
                                        notify_terminal(j, Kind::Completed, "");
                                    } else if (client.queued_messages() < 8) {
                                        const auto now = ticks();
                                        if (!j.last_progress || now - j.last_progress >= progress_interval) {
                                            reply(progress);
                                            j.last_progress = now;
                                        }
                                    }
                                } catch (const std::exception &e) {
                                    fail(j, e.what());
                                }
                        } else
                            trace.log("result_discarded", m, name(j.state));
                        settle(j);
                    } else if (m.kind == Kind::ShutdownAck && w.shutdown_sent && !w.stopped) {
                        w.stopped = true;
                        trace.log("worker_stopped", m);
                    } else
                        throw std::runtime_error("Message not allowed from Worker");
                }
                if (w.channel.closed() && !w.stopped)
                    throw std::runtime_error("Worker disconnected");
            } catch (const std::exception &e) {
                lose_worker(w, e.what());
            }
        }
        settle_disconnect();
        // [Flow 7] เลือก Job หน้าคิวตาม FIFO/RR แล้วส่ง TASK (chunk ถัดไป) ให้ Worker ที่ Ready → Flow 8 Worker
        // 3. Dispatch only to free Workers. Policy changes Job selection, never
        //    preempts computation or changes the order in which results arrive.
        for (auto &owned : workers) {
            auto &w = *owned;
            if (w.state != WorkerState::Ready || w.shutdown_sent || ready.empty())
                continue;
            auto id = ready.front();
            auto &j = jobs.at(id);
            if (!j.pending_chunks()) {
                ready.pop_front();
                continue;
            }
            auto task = j.base;
            task.kind = Kind::Task;
            task.chunk = j.next + 1;
            task.worker = w.id;
            task.begin = j.index.chunks[j.next].begin;
            task.end = j.index.chunks[j.next].end;
            ++j.next;
            ++j.in_flight;
            j.state = JobState::Active;
            if (!j.first)
                j.first = ticks();
            w.assignment = task;
            w.state = WorkerState::Busy;
            if (policy == "rr" || j.next == j.index.chunks.size()) {
                ready.pop_front();
                if (j.next < j.index.chunks.size())
                    ready.push_back(id);
            }
            trace.log("task_assigned", task, policy + ";worker_free;chunk_boundary");
            try {
                w.channel.send(task);
            } catch (const std::exception &e) {
                lose_worker(w, e.what());
            }
        }
        // [Flow 11] เมื่อทุก Job settle แล้ว ส่ง SHUTDOWN ให้ Workers รอ ACK แล้วตอบ SHUTDOWN_ACK ให้ Client
        // 4. Shut down only after terminal Jobs have no outstanding assignments.
        bool settled = std::all_of(jobs.begin(), jobs.end(), [](const auto &entry) { return entry.second.settled(); });
        if (stopping && settled) {
            if (!shutdown_handshake_deadline)
                shutdown_handshake_deadline = GetTickCount64() + 1500;
            for (auto &owned : workers) {
                auto &w = *owned;
                if (w.shutdown_sent || w.stopped)
                    continue;
                // A Client can request shutdown before an already-starting Worker
                // delivers READY. Let the asynchronous handshake finish first.
                if (!w.seen && GetTickCount64() < shutdown_handshake_deadline)
                    continue;
                if (w.channel.connected() && w.seen) {
                    Message m;
                    m.kind = Kind::Shutdown;
                    m.request = shutdown_request;
                    m.worker = w.id;
                    try {
                        w.channel.send(m);
                        w.shutdown_sent = true;
                    } catch (const std::exception &e) {
                        lose_worker(w, e.what());
                    }
                } else {
                    w.channel.close();
                    w.stopped = true;
                }
            }
            if (std::all_of(workers.begin(), workers.end(), [](const auto &w) { return w->stopped; })) {
                if (shutdown_request && !shutdown_reply_sent) {
                    Message m;
                    m.kind = Kind::ShutdownAck;
                    m.request = shutdown_request;
                    reply(m);
                    shutdown_reply_sent = true;
                }
                if (client_lost || !client.connected() || client.drained())
                    break;
            }
        }
        // 5. Sleep until any relevant I/O/control event is ready.
        std::vector<HANDLE> events{stop.get()};
        // Deliberately sequential experimental baseline; normal mode waits on all peers.
        auto busy =
            std::find_if(workers.begin(), workers.end(), [](const auto &w) { return w->state == WorkerState::Busy; });
        if (io_mode == "sequential-wait" && busy != workers.end())
            (*busy)->channel.append_events(events);
        else {
            client.append_events(events);
            for (const auto &w : workers)
                w->channel.append_events(events);
        }
        const auto now = GetTickCount64();
        const DWORD timeout = shutdown_handshake_deadline && now < shutdown_handshake_deadline
                                  ? static_cast<DWORD>(shutdown_handshake_deadline - now)
                                  : INFINITE;
        wait_events(events, timeout);
    }
    trace.log("broker_stopped");
    stop_event = nullptr;
    SetConsoleCtrlHandler(interrupt, FALSE);
    return 0;
} catch (const std::exception &e) {
    std::cerr << "broker: " << e.what() << std::endl;
    return 1;
}
