#include "analyzer.hpp"
#include "message.hpp"
#include "platform.hpp"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <limits>
using namespace lga;
void require(bool ok, const char *reason) {
    if (!ok)
        throw std::runtime_error(reason);
}
template <class F> void rejects(F action) {
    bool failed = false;
    try {
        action();
    } catch (const std::exception &) {
        failed = true;
    }
    require(failed, "Expected rejection");
}
int main() try {
    auto route = analyze("data/small.csv", Analysis::Route, std::filesystem::file_size("data/small.csv"));
    require(route.valid == 7 && route.malformed == 3, "record accounting");
    require(route.groups.size() == 2 && route.groups.at("/login").errors == 2 &&
                route.groups.at("/payment").errors5xx == 2,
            "route counts");
    auto time = analyze("data/small.csv", Analysis::Time, std::filesystem::file_size("data/small.csv"));
    require(time.groups.size() == 3 && time.groups.at("2026-10-11T10:01:00Z").errors == 2, "minute boundary");
    std::filesystem::create_directories("build/test-input");
    auto file = std::filesystem::path("build/test-input/boundary.csv");
    {
        std::ofstream f(file, std::ios::binary);
        f << "\xef\xbb\xbftimestamp,route,status\r\n2024-02-29T23:59:59Z,/a,599\r\n\r\n2023-02-29T00:00:00Z,/"
             "a,400\r\n2026-10-11T10:00:00Z,/last,400";
    }
    auto boundary = analyze(file, Analysis::Time, std::filesystem::file_size(file));
    require(boundary.valid == 2 && boundary.malformed == 2 && boundary.groups.size() == 2,
            "CRLF/BOM/leap/blank/final-line handling");
    {
        std::ofstream f(file, std::ios::binary);
        f << "timestamp,route,status\n"
             "2026-10-11T10:00:00Z,/a,399\n2026-10-11T10:00:00Z,/a,400\n2026-10-11T10:00:00Z,/a,100\n"
             "2026-10-11T10:00:00Z,/a,600\n2026-10-11T10:00:00Z,/has space,500\n"
             "2026-10-11T10:00:00Z,/a,500,extra\n2026-10-11T10:00:00Z,/";
        f.put(static_cast<char>(0xff));
        f << ",500\n";
    }
    auto invalid_records = analyze(file, Analysis::Route, std::filesystem::file_size(file));
    require(invalid_records.valid == 3 && invalid_records.malformed == 4 && invalid_records.groups.at("/a").errors == 1,
            "status boundary/invalid UTF-8/extra field/route whitespace");
    {
        std::ofstream f(file);
        f << "timestamp,route,status\n";
    }
    require(analyze(file, Analysis::Route, std::filesystem::file_size(file)).valid == 0, "header-only input");
    { std::ofstream f(file); }
    rejects([&] { analyze(file, Analysis::Route, 0); });
    {
        std::ofstream f(file);
        f << "wrong header\n";
    }
    rejects([&] { analyze(file, Analysis::Route, std::filesystem::file_size(file)); });
    Message m;
    m.kind = Kind::Task;
    m.request = 42;
    m.job = 7;
    m.chunk = 1;
    m.worker = 1;
    m.path = "C:/a space/input.csv";
    m.end = 80;
    const auto frame = encode(m);
    Decoder decoder;
    std::vector<Message> received;
    for (auto byte : frame) {
        auto batch = decoder.feed(&byte, 1);
        received.insert(received.end(), batch.begin(), batch.end());
    }
    require(received.size() == 1 && received[0].path == m.path && received[0].request == 42, "fragmented frame");
    auto twice = frame;
    twice.insert(twice.end(), frame.begin(), frame.end());
    Decoder coalesced;
    require(coalesced.feed(twice.data(), twice.size()).size() == 2, "coalesced frames");
    auto bad = frame;
    bad[0] = 0;
    rejects([&] {
        Decoder d;
        d.feed(bad.data(), bad.size());
    });
    bad = frame;
    bad[4] = 1;
    rejects([&] {
        Decoder d;
        d.feed(bad.data(), bad.size());
    });
    bad = frame;
    bad[11] = 0x7f;
    rejects([&] {
        Decoder d;
        d.feed(bad.data(), 12);
    });
    m.kind = Kind::Result;
    m.summary = route;
    Decoder result_decoder;
    auto result_frame = encode(m);
    auto decoded = result_decoder.feed(result_frame.data(), result_frame.size());
    require(decoded.size() == 1 && summary_json(decoded[0].summary) == summary_json(route), "result serialization");
    m.summary.valid = 0;
    auto too_many_errors = encode(m);
    rejects([&] {
        Decoder d;
        d.feed(too_many_errors.data(), too_many_errors.size());
    });
    m.summary.valid = route.valid;
    m.summary.groups.at("/login").errors = 0;
    auto invalid = encode(m);
    rejects([&] {
        Decoder d;
        d.feed(invalid.data(), invalid.size());
    });
    // Coverage and result accounting across every possible scheduling order.
    const std::string records = "\xef\xbb\xbftimestamp,route,status\r\n2024-02-29T23:59:59Z,/a,599\r\n\r\n"
                                "bad\n2026-10-11T10:00:00Z,/a,200\n2026-10-11T10:01:00Z,/last,400";
    {
        std::ofstream out(file, std::ios::binary);
        out << records;
    }
    for (const auto target : {1ULL, 32ULL, 70ULL, 10000ULL}) {
        const auto prepared = prepare_index(file, target);
        LockedInput input(file);
        const auto index = load_index(file, input);
        require(index.chunks.size() == prepared.chunks.size() && index.chunks.front().begin == 0 &&
                    index.chunks.back().end == records.size(),
                "index covers full original file");
        std::uint64_t position = 0;
        for (const auto &chunk : index.chunks) {
            require(chunk.begin == position && chunk.end > chunk.begin, "no chunk gap/overlap/empty chunk");
            position = chunk.end;
        }
        for (const auto mode : {Analysis::Route, Analysis::Time}) {
            Summary combined;
            for (auto it = index.chunks.rbegin(); it != index.chunks.rend(); ++it)
                merge_summary(combined, analyze_chunk(file, mode, it->begin, it->end, index.stamp));
            require(summary_json(combined) == summary_json(analyze(file, mode, records.size())),
                    "out-of-order chunk/reference agreement");
            require(combined.valid == 3 && combined.malformed == 2, "all physical records accounted for");
        }
        Handle writer(CreateFileW(file.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                                  OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr));
        require(!writer && GetLastError() == ERROR_SHARING_VIOLATION, "input lock denies concurrent writer");
        require(!DeleteFileW(file.c_str()) && GetLastError() == ERROR_SHARING_VIOLATION,
                "input lock denies path replacement");
        auto wrong = index.stamp;
        wrong.file_id ^= 1;
        rejects([&] { analyze_chunk(file, Analysis::Route, 0, records.size(), wrong); });
        rejects([&] { analyze_chunk(file, Analysis::Route, 1, records.size(), index.stamp); });
        rejects([&] { analyze_chunk(file, Analysis::Route, 0, records.size() - 1, index.stamp); });
    }
    // A stale sidecar must not silently map a modified input.
    prepare_index(file, 32);
    {
        std::ofstream out(file, std::ios::binary | std::ios::app);
        out << '\n';
    }
    {
        LockedInput input(file);
        rejects([&] { load_index(file, input); });
    }
    {
        std::ofstream out(file, std::ios::binary);
        out << "timestamp,route,status\n";
    }
    const auto only_header = prepare_index(file, 1);
    require(only_header.chunks.size() == 1 &&
                analyze_chunk(file, Analysis::Route, 0, only_header.stamp.size, only_header.stamp).valid == 0,
            "header-only single empty-data chunk");
    {
        std::fstream sidecar(index_path(file), std::ios::binary | std::ios::in | std::ios::out);
        sidecar.seekp(56);
        sidecar.put(1);
    }
    {
        LockedInput input(file);
        rejects([&] { load_index(file, input); });
    }
    { std::ofstream out(file, std::ios::binary); }
    rejects([&] { prepare_index(file, 10); });
    {
        std::ofstream out(file, std::ios::binary);
        out << "timestamp,route,status\n" << std::string(200000, 'x') << "\n2026-10-11T10:00:00Z,/ok,500";
    }
    const auto oversized_line = prepare_index(file, 16);
    require(oversized_line.chunks.size() == 2, "giant malformed line remains one record");
    Summary giant;
    for (const auto &chunk : oversized_line.chunks)
        merge_summary(giant, analyze_chunk(file, Analysis::Route, chunk.begin, chunk.end, oversized_line.stamp));
    require(giant.valid == 1 && giant.malformed == 1, "bounded parser handles giant malformed line");
    rejects([&] { prepare_index(file, 0); });
    Summary full, extra;
    full.valid = max_groups;
    for (std::size_t i = 0; i < max_groups; ++i)
        full.groups["/key" + std::to_string(i)] = {1, 1, 0};
    extra.valid = 1;
    extra.groups["/new"] = {1, 0, 1};
    const auto saved = summary_json(full);
    rejects([&] { merge_summary(full, extra); });
    require(summary_json(full) == saved, "failed group-limit merge is atomic");
    Summary overflow;
    overflow.valid = std::numeric_limits<std::uint64_t>::max();
    rejects([&] { merge_summary(overflow, extra); });
    require(overflow.groups.empty(), "overflow rejected before mutation");
    // All v2 message extensions survive fragmented transport.
    Message task;
    task.kind = Kind::Task;
    task.request = 5;
    task.job = 6;
    task.chunk = 9;
    task.worker = 3;
    task.path = "C:/test.csv";
    task.begin = 100;
    task.end = 400;
    task.file_size = 900;
    task.file_mtime = 123456;
    task.file_volume = 7;
    task.file_id = 99;
    auto task_frame = encode(task);
    Decoder td;
    const auto task_copy = td.feed(task_frame.data(), task_frame.size()).at(0);
    require(task_copy.begin == 100 && task_copy.end == 400 && task_copy.file_size == 900 && task_copy.file_id == 99 &&
                task_copy.file_volume == 7 && task_copy.file_mtime == 123456,
            "task snapshot serialization");
    for (const auto kind : {Kind::Status, Kind::Progress, Kind::CancelAck, Kind::Cancelled}) {
        Message progress;
        progress.kind = kind;
        progress.request = 77;
        progress.job = 8;
        progress.state = JobState::Cancelling;
        progress.total_chunks = 8;
        progress.completed_chunks = 2;
        progress.in_flight = 3;
        progress.pending_chunks = 0;
        progress.monitor = "{\"jobs\":[],\"workers\":[]}";
        const auto bytes = encode(progress);
        Decoder d;
        auto result = d.feed(bytes.data(), bytes.size()).at(0);
        require(result.total_chunks == 8 && result.completed_chunks == 2 && result.in_flight == 3 &&
                    result.monitor == progress.monitor,
                "progress/cancellation serialization");
        progress.completed_chunks = 9;
        const auto corrupt = encode(progress);
        rejects([&] {
            Decoder invalid_decoder;
            invalid_decoder.feed(corrupt.data(), corrupt.size());
        });
    }
    Message accepted;
    accepted.kind = Kind::Accepted;
    accepted.total_chunks = 42;
    auto accepted_bytes = encode(accepted);
    Decoder ad;
    require(ad.feed(accepted_bytes.data(), accepted_bytes.size()).at(0).total_chunks == 42,
            "accepted expected chunk count");
    Message cancel;
    cancel.kind = Kind::Cancel;
    cancel.request = 11;
    cancel.job = 22;
    auto cancel_bytes = encode(cancel);
    Decoder cd;
    require(cd.feed(cancel_bytes.data(), cancel_bytes.size()).at(0).job == 22, "cancel target serialization");
    std::cout << "Common contract/analyzer tests passed" << std::endl;
    return 0;
} catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
}
