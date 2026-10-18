#pragma once
#include "model.hpp"
#include "chunks.hpp"
#include <filesystem>
namespace lga {
Summary analyze(const std::filesystem::path &path, Analysis analysis, std::uint64_t expected_size);
Summary analyze_chunk(const std::filesystem::path &path, Analysis analysis, std::uint64_t begin, std::uint64_t end,
                      const FileStamp &expected);
void merge_summary(Summary &destination, const Summary &source);
std::string summary_json(const Summary &s);
} // namespace lga
