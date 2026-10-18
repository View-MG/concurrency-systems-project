#pragma once
#include "model.hpp"
#include "platform.hpp"
#include <algorithm>
#include <array>
#include <vector>

namespace lga {
struct FileStamp {
    std::uint64_t size = 0, mtime = 0, volume = 0, file_id = 0;
};
inline bool operator==(const FileStamp &a, const FileStamp &b) {
    return a.size == b.size && a.mtime == b.mtime && a.volume == b.volume && a.file_id == b.file_id;
}
inline bool operator!=(const FileStamp &a, const FileStamp &b) {
    return !(a == b);
}
struct ChunkRange {
    std::uint64_t begin = 0, end = 0;
};
struct ChunkIndex {
    FileStamp stamp;
    std::uint64_t target_bytes = 0;
    std::vector<ChunkRange> chunks;
};
// Keep this owner alive until every assignment is settled. FILE_SHARE_READ denies
// writers and deletion while allowing all Workers to open their own read handles.
class LockedInput {
    Handle handle_;
    FileStamp stamp_;

  public:
    explicit LockedInput(const std::filesystem::path &path);
    const FileStamp &stamp() const { return stamp_; }
    void check_unchanged() const;
    std::size_t read_at(std::uint64_t offset, char *buffer, std::size_t capacity) const;
};
std::filesystem::path index_path(const std::filesystem::path &path);
ChunkIndex prepare_index(const std::filesystem::path &path, std::uint64_t target_bytes);
ChunkIndex load_index(const std::filesystem::path &path, const LockedInput &input);

// Streaming physical lines with bounded memory even for a malformed giant line.
// Offsets include LF when present. A trailing LF does not create another record.
class RecordReader {
    const LockedInput &input_;
    std::uint64_t position_, end_;
    std::array<char, 65536> buffer_{};
    std::size_t cursor_ = 0, available_ = 0;

  public:
    RecordReader(const LockedInput &input, std::uint64_t begin, std::uint64_t end)
        : input_(input), position_(begin), end_(end) {}
    std::uint64_t position() const { return position_; }
    bool next(std::string &text) {
        text.clear();
        if (position_ == end_)
            return false;
        while (position_ < end_) {
            if (cursor_ == available_) {
                available_ =
                    input_.read_at(position_, buffer_.data(),
                                   static_cast<std::size_t>(std::min<std::uint64_t>(buffer_.size(), end_ - position_)));
                cursor_ = 0;
                if (!available_)
                    throw std::runtime_error("Unexpected end of input");
            }
            const char c = buffer_[cursor_++];
            ++position_;
            if (c == '\n')
                return true;
            if (text.size() < 1026)
                text.push_back(c);
        }
        return true;
    }
};
inline void validate_header(std::string line) {
    if (!line.empty() && line.back() == '\r')
        line.pop_back();
    if (line.rfind("\xef\xbb\xbf", 0) == 0)
        line.erase(0, 3);
    if (line != "timestamp,route,status")
        throw std::runtime_error("Expected CSV header timestamp,route,status");
}
} // namespace lga
