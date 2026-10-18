#include "chunks.hpp"
#include <iostream>
using namespace lga;
int main() try {
    Options options;
    options.allow({"--file", "--chunk-bytes"});
    const auto bytes = options.number("--chunk-bytes", 1048576, 1, max_file_bytes);
    const auto path = std::filesystem::u8path(options.get("--file"));
    const auto start = ticks();
    const auto index = prepare_index(path, bytes);
    std::cout << "{\"index\":" << quote_json(index_path(path).u8string()) << ",\"file_size\":" << index.stamp.size
              << ",\"chunk_bytes\":" << index.target_bytes << ",\"total_chunks\":" << index.chunks.size()
              << ",\"prepare_us\":" << elapsed_us(start) << "}" << std::endl;
    return 0;
} catch (const std::exception &e) {
    std::cerr << "prepare: " << e.what() << std::endl;
    return 1;
}
