#pragma once
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601
#endif
#include <windows.h>
#include <shellapi.h>
#include <cstdint>
#include <filesystem>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

namespace lga {
inline std::runtime_error win_error(const char *operation, DWORD code = GetLastError()) {
    return std::runtime_error(std::string(operation) + " (Win32 error " + std::to_string(code) + ")");
}
class Handle {
    HANDLE h_ = INVALID_HANDLE_VALUE;

  public:
    Handle() = default;
    explicit Handle(HANDLE h) : h_(h) {}
    ~Handle() { reset(); }
    Handle(const Handle &) = delete;
    Handle &operator=(const Handle &) = delete;
    Handle(Handle &&x) noexcept : h_(std::exchange(x.h_, INVALID_HANDLE_VALUE)) {}
    Handle &operator=(Handle &&x) noexcept {
        reset(std::exchange(x.h_, INVALID_HANDLE_VALUE));
        return *this;
    }
    HANDLE get() const { return h_; }
    explicit operator bool() const { return h_ && h_ != INVALID_HANDLE_VALUE; }
    void reset(HANDLE h = INVALID_HANDLE_VALUE) {
        if (*this)
            CloseHandle(h_);
        h_ = h;
    }
};
inline Handle event() {
    Handle h(CreateEventW(nullptr, TRUE, FALSE, nullptr));
    if (!h)
        throw win_error("CreateEvent");
    return h;
}
inline std::uint64_t ticks() {
    LARGE_INTEGER q{};
    if (!QueryPerformanceCounter(&q))
        throw win_error("QueryPerformanceCounter");
    return static_cast<std::uint64_t>(q.QuadPart);
}
inline std::uint64_t frequency() {
    LARGE_INTEGER q{};
    if (!QueryPerformanceFrequency(&q))
        throw win_error("QueryPerformanceFrequency");
    return static_cast<std::uint64_t>(q.QuadPart);
}
inline std::uint64_t elapsed_us(std::uint64_t start) {
    return static_cast<std::uint64_t>((ticks() - start) * 1000000.0 / frequency());
}
inline bool valid_utf8(const std::string &s) {
    if (s.empty())
        return true;
    return MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s.data(), static_cast<int>(s.size()), nullptr, 0) > 0;
}
inline std::wstring wide(const std::string &s) {
    const int n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s.data(), static_cast<int>(s.size()), nullptr, 0);
    if (!n && !s.empty())
        throw win_error("MultiByteToWideChar");
    std::wstring w(static_cast<std::size_t>(n), L'\0');
    if (n)
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s.data(), static_cast<int>(s.size()), w.data(), n);
    return w;
}
inline std::string utf8(const std::wstring &w) {
    const int n = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, w.data(), static_cast<int>(w.size()), nullptr, 0,
                                      nullptr, nullptr);
    if (!n && !w.empty())
        throw win_error("WideCharToMultiByte");
    std::string s(static_cast<std::size_t>(n), '\0');
    if (n)
        WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, w.data(), static_cast<int>(w.size()), s.data(), n, nullptr,
                            nullptr);
    return s;
}
inline std::string quote_json(const std::string &s) {
    std::string out = "\"";
    constexpr char hex[] = "0123456789abcdef";
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 0x20) {
            out += "\\u00";
            out += hex[c >> 4];
            out += hex[c & 15];
        } else
            out += static_cast<char>(c);
    }
    return out + '"';
}
struct Options {
    std::map<std::string, std::string> values;
    Options() {
        // CRT char** uses the machine ANSI code page. Read the original UTF-16
        // command line so Prepare/Reference/trace paths also support Thai.
        int count = 0;
        struct Arguments {
            LPWSTR *values;
            ~Arguments() {
                if (values)
                    LocalFree(values);
            }
        } arguments{CommandLineToArgvW(GetCommandLineW(), &count)};
        if (!arguments.values)
            throw win_error("CommandLineToArgvW");
        for (int i = 1; i < count; i += 2) {
            const auto key = utf8(arguments.values[i]);
            if (i + 1 == count || key.rfind("--", 0) != 0)
                throw std::runtime_error("Options use --name value pairs");
            if (!values.emplace(key, utf8(arguments.values[i + 1])).second)
                throw std::runtime_error("Duplicate option");
        }
    }
    std::string get(const std::string &key, const std::string &fallback = "") const {
        auto it = values.find(key);
        return it == values.end() ? fallback : it->second;
    }
    std::uint64_t number(const std::string &key, std::uint64_t fallback, std::uint64_t low, std::uint64_t high) const {
        const auto text = get(key, std::to_string(fallback));
        if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
            throw std::runtime_error("Invalid " + key);
        const auto value = std::stoull(text);
        if (value < low || value > high)
            throw std::runtime_error("Out of range " + key);
        return value;
    }
    void allow(std::initializer_list<std::string> keys) const {
        for (const auto &[key, value] : values) {
            bool found = false;
            for (const auto &allowed : keys)
                if (key == allowed)
                    found = true;
            if (!found)
                throw std::runtime_error("Unknown option: " + key);
        }
    }
};
inline std::wstring pipe_name(const std::string &session, const char *role) {
    if (session.empty() || session.size() > 48)
        throw std::runtime_error("Session length must be 1..48");
    for (char c : session)
        if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_')
            throw std::runtime_error("Invalid session name");
    return wide("\\\\.\\pipe\\lga_" + session + "_" + role);
}
} // namespace lga
