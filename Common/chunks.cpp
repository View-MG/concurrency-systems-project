#include "chunks.hpp"
#include <fstream>
#include <limits>
#include <sstream>

namespace lga {
namespace {
FileStamp file_stamp(HANDLE handle) {
    BY_HANDLE_FILE_INFORMATION info{};
    if (!GetFileInformationByHandle(handle, &info))
        throw win_error("GetFileInformationByHandle");
    if (GetFileType(handle) != FILE_TYPE_DISK || (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
        throw std::runtime_error("Input must be a regular disk file");
    return {(static_cast<std::uint64_t>(info.nFileSizeHigh) << 32) | info.nFileSizeLow,
            (static_cast<std::uint64_t>(info.ftLastWriteTime.dwHighDateTime) << 32) |
                info.ftLastWriteTime.dwLowDateTime,
            info.dwVolumeSerialNumber, (static_cast<std::uint64_t>(info.nFileIndexHigh) << 32) | info.nFileIndexLow};
}
void put(std::ostream &out, std::uint64_t value, unsigned width = 8) {
    for (unsigned i = 0; i < width; ++i)
        out.put(static_cast<char>(value >> (8 * i)));
}
std::uint64_t get(std::istream &in, unsigned width = 8) {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) {
        const int c = in.get();
        if (c == EOF)
            throw std::runtime_error("Truncated chunk index; run prepare again");
        value |= static_cast<std::uint64_t>(static_cast<unsigned char>(c)) << (8 * i);
    }
    return value;
}
constexpr std::uint64_t index_header_bytes = 56;
} // namespace
LockedInput::LockedInput(const std::filesystem::path &path)
    : handle_(CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
                          nullptr)) {
    if (!handle_)
        throw win_error("Open stable read-only input");
    stamp_ = file_stamp(handle_.get());
    if (stamp_.size > max_file_bytes)
        throw std::runtime_error("Input exceeds 8 GiB limit");
}
void LockedInput::check_unchanged() const {
    if (file_stamp(handle_.get()) != stamp_)
        throw std::runtime_error("Input changed while locked");
}
std::size_t LockedInput::read_at(std::uint64_t offset, char *buffer, std::size_t capacity) const {
    if (offset > stamp_.size || capacity > std::numeric_limits<DWORD>::max())
        throw std::runtime_error("Invalid input read range");
    LARGE_INTEGER position{};
    position.QuadPart = static_cast<LONGLONG>(offset);
    if (!SetFilePointerEx(handle_.get(), position, nullptr, FILE_BEGIN))
        throw win_error("SetFilePointerEx");
    DWORD read = 0;
    if (!ReadFile(handle_.get(), buffer, static_cast<DWORD>(capacity), &read, nullptr))
        throw win_error("Read input");
    return read;
}
std::filesystem::path index_path(const std::filesystem::path &path) {
    auto result = path;
    result += L".lgaidx";
    return result;
}
ChunkIndex prepare_index(const std::filesystem::path &path, std::uint64_t target_bytes) {
    if (!target_bytes || target_bytes > max_file_bytes)
        throw std::runtime_error("chunk-bytes must be 1..8589934592");
    LockedInput input(path);
    ChunkIndex index;
    index.stamp = input.stamp();
    index.target_bytes = target_bytes;
    RecordReader lines(input, 0, index.stamp.size);
    std::string line;
    if (!lines.next(line))
        throw std::runtime_error("Missing CSV header");
    validate_header(line);
    std::uint64_t begin = 0;
    const auto append = [&](std::uint64_t end) {
        if (index.chunks.size() >= max_chunks)
            throw std::runtime_error("Chunk limit exceeded; increase chunk-bytes");
        index.chunks.push_back({begin, end});
        begin = end;
    };
    while (lines.next(line))
        if (lines.position() - begin >= target_bytes)
            append(lines.position());
    if (begin < index.stamp.size)
        append(index.stamp.size);
    input.check_unchanged();
    auto destination = index_path(path), temporary = destination;
    temporary += L".tmp." + std::to_wstring(GetCurrentProcessId()) + L"." + std::to_wstring(ticks());
    try {
        std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
        if (!out)
            throw std::runtime_error("Cannot write chunk index");
        put(out, 0x3249474c, 4);
        put(out, 1, 4);
        put(out, index.stamp.size);
        put(out, index.stamp.mtime);
        put(out, index.stamp.volume);
        put(out, index.stamp.file_id);
        put(out, index.target_bytes);
        put(out, index.chunks.size());
        for (const auto &chunk : index.chunks) {
            put(out, chunk.begin);
            put(out, chunk.end);
        }
        out.flush();
        if (!out)
            throw std::runtime_error("Cannot flush chunk index");
        out.close();
        if (!out)
            throw std::runtime_error("Cannot close chunk index");
        if (!MoveFileExW(temporary.c_str(), destination.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
            throw win_error("Publish chunk index");
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(temporary, ec);
        throw;
    }
    return index;
}
ChunkIndex load_index(const std::filesystem::path &path, const LockedInput &input) {
    const auto sidecar = index_path(path);
    std::error_code ec;
    const auto bytes = std::filesystem::file_size(sidecar, ec);
    if (ec)
        throw std::runtime_error("Missing chunk index; run prepare --file <path> --chunk-bytes <bytes>");
    if (bytes < index_header_bytes + 16 || bytes > index_header_bytes + max_chunks * 16)
        throw std::runtime_error("Invalid chunk index size; run prepare again");
    // Lock metadata too so concurrent preparation cannot replace the file halfway
    // through admission. This read remains bounded by 1 MiB plus 56 bytes.
    LockedInput metadata(sidecar);
    if (metadata.stamp().size != bytes)
        throw std::runtime_error("Chunk index changed during admission; retry");
    std::vector<char> data(static_cast<std::size_t>(metadata.stamp().size));
    if (metadata.read_at(0, data.data(), data.size()) != data.size())
        throw std::runtime_error("Cannot read chunk index");
    std::istringstream in(std::string(data.begin(), data.end()), std::ios::binary);
    if (get(in, 4) != 0x3249474c || get(in, 4) != 1)
        throw std::runtime_error("Invalid chunk index magic/version");
    ChunkIndex index;
    index.stamp = {get(in), get(in), get(in), get(in)};
    index.target_bytes = get(in);
    const auto count = get(in);
    if (index.stamp != input.stamp())
        throw std::runtime_error("Stale chunk index (identity/size/mtime); run prepare again");
    if (!index.stamp.size || !index.target_bytes || index.target_bytes > max_file_bytes || !count ||
        count > max_chunks || bytes != index_header_bytes + count * 16)
        throw std::runtime_error("Invalid chunk index fields");
    index.chunks.reserve(static_cast<std::size_t>(count));
    std::uint64_t next = 0;
    for (std::uint64_t i = 0; i < count; ++i) {
        ChunkRange chunk{get(in), get(in)};
        if (chunk.begin != next || chunk.end <= chunk.begin || chunk.end > index.stamp.size)
            throw std::runtime_error("Chunk index has overlap/gap/invalid range");
        index.chunks.push_back(chunk);
        next = chunk.end;
    }
    if (next != index.stamp.size)
        throw std::runtime_error("Chunk index does not cover input");
    input.check_unchanged();
    return index;
}
} // namespace lga
