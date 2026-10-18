#include "analyzer.hpp"
#include "platform.hpp"
#include <array>
#include <sstream>
#include <limits>

namespace lga {
namespace {
bool timestamp(const std::string &s) {
    if (s.size() != 20 || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' || s[19] != 'Z')
        return false;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (i == 4 || i == 7 || i == 10 || i == 13 || i == 16 || i == 19)
            continue;
        if (s[i] < '0' || s[i] > '9')
            return false;
    }
    const int y = std::stoi(s.substr(0, 4)), m = std::stoi(s.substr(5, 2)), d = std::stoi(s.substr(8, 2));
    const std::array<int, 12> days{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1970 || m < 1 || m > 12)
        return false;
    const bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    return d >= 1 && d <= days[static_cast<std::size_t>(m - 1)] + (m == 2 && leap ? 1 : 0) &&
           std::stoi(s.substr(11, 2)) < 24 && std::stoi(s.substr(14, 2)) < 60 && std::stoi(s.substr(17, 2)) < 60;
}
Summary analyze_locked(const LockedInput &file, Analysis analysis, std::uint64_t begin, std::uint64_t end) {
    if (analysis != Analysis::Route && analysis != Analysis::Time)
        throw std::runtime_error("Invalid analysis type");
    if (begin > end || end > file.stamp().size)
        throw std::runtime_error("Invalid chunk byte range");
    RecordReader header(file, 0, file.stamp().size);
    std::string line;
    if (!header.next(line))
        throw std::runtime_error("Missing CSV header");
    validate_header(line);
    if (begin && begin < header.position())
        throw std::runtime_error("Chunk begins inside CSV header");
    const auto boundary = [&](std::uint64_t offset) {
        if (offset && offset < file.stamp().size) {
            char c = 0;
            if (file.read_at(offset - 1, &c, 1) != 1 || c != '\n')
                throw std::runtime_error("Chunk boundary splits a record");
        }
    };
    boundary(begin);
    boundary(end);
    if (!begin && end < header.position())
        throw std::runtime_error("Chunk ends inside CSV header");
    RecordReader records(file, begin ? begin : header.position(), end);
    Summary result;
    while (records.next(line)) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        const auto a = line.find(','), b = a == std::string::npos ? a : line.find(',', a + 1);
        bool ok = line.size() <= 1024 && valid_utf8(line) && a != std::string::npos && b != std::string::npos &&
                  line.find(',', b + 1) == std::string::npos;
        std::string time, route, status;
        if (ok) {
            time = line.substr(0, a);
            route = line.substr(a + 1, b - a - 1);
            status = line.substr(b + 1);
            ok = timestamp(time) && !route.empty() && route.front() == '/' && route.size() <= 256 && status.size() == 3;
            for (unsigned char c : route)
                if (c <= 0x20 || c == 0x7f || c == '"')
                    ok = false;
            for (char c : status)
                if (c < '0' || c > '9')
                    ok = false;
        }
        const int code = ok ? std::stoi(status) : 0;
        if (!ok || code < 100 || code > 599) {
            ++result.malformed;
            continue;
        }
        ++result.valid;
        if (code < 400)
            continue;
        const std::string key = analysis == Analysis::Route ? route : time.substr(0, 16) + ":00Z";
        if (!result.groups.count(key) && result.groups.size() >= max_groups)
            throw std::runtime_error("Aggregation key limit exceeded");
        auto &count = result.groups[key];
        ++count.errors;
        if (code < 500)
            ++count.errors4xx;
        else
            ++count.errors5xx;
    }
    file.check_unchanged();
    return result;
}
} // namespace

Summary analyze(const std::filesystem::path &path, Analysis analysis, std::uint64_t expected_size) {
    LockedInput input(path);
    if (input.stamp().size != expected_size)
        throw std::runtime_error("Input size changed before analysis");
    return analyze_locked(input, analysis, 0, expected_size);
}
Summary analyze_chunk(const std::filesystem::path &path, Analysis analysis, std::uint64_t begin, std::uint64_t end,
                      const FileStamp &expected) {
    LockedInput input(path);
    if (input.stamp() != expected)
        throw std::runtime_error("Input identity/size/mtime differs from assignment");
    return analyze_locked(input, analysis, begin, end);
}
void merge_summary(Summary &destination, const Summary &source) {
    const auto add = [](std::uint64_t a, std::uint64_t b) {
        if (b > std::numeric_limits<std::uint64_t>::max() - a)
            throw std::runtime_error("Summary count overflow");
        return a + b;
    };
    // Validate every addition before changing the destination, including new-key
    // capacity. A rejected merge cannot leave a partially accepted chunk result.
    add(destination.valid, source.valid);
    add(destination.malformed, source.malformed);
    std::uint64_t errors_left = source.valid;
    std::size_t new_keys = 0;
    for (const auto &[key, count] : source.groups) {
        if (key.empty() || key.size() > 256 || !valid_utf8(key) || count.errors > errors_left ||
            count.errors4xx > count.errors || count.errors5xx != count.errors - count.errors4xx)
            throw std::runtime_error("Invalid source summary");
        errors_left -= count.errors;
        const auto it = destination.groups.find(key);
        if (it == destination.groups.end())
            ++new_keys;
        else {
            add(it->second.errors, count.errors);
            add(it->second.errors4xx, count.errors4xx);
            add(it->second.errors5xx, count.errors5xx);
        }
    }
    if (destination.groups.size() + new_keys > max_groups)
        throw std::runtime_error("Aggregation key limit exceeded");
    destination.valid += source.valid;
    destination.malformed += source.malformed;
    for (const auto &[key, count] : source.groups) {
        auto &target = destination.groups[key];
        target.errors += count.errors;
        target.errors4xx += count.errors4xx;
        target.errors5xx += count.errors5xx;
    }
}
std::string summary_json(const Summary &s) {
    std::ostringstream out;
    out << "{\"valid_records\":" << s.valid << ",\"malformed_records\":" << s.malformed << ",\"groups\":{";
    bool first = true;
    for (const auto &[key, count] : s.groups) {
        if (!first)
            out << ',';
        first = false;
        out << quote_json(key) << ":{\"errors\":" << count.errors << ",\"4xx\":" << count.errors4xx
            << ",\"5xx\":" << count.errors5xx << '}';
    }
    return out.str() + "}}";
}
} // namespace lga
