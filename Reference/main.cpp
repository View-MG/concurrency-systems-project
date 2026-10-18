#include "analyzer.hpp"
#include "platform.hpp"
#include <iostream>
#include <fstream>
using namespace lga;
int main() try {
    Options options;
    options.allow({"--file", "--analysis", "--timing-file"});
    const auto mode = options.get("--analysis", "route");
    if (mode != "route" && mode != "time")
        throw std::runtime_error("Analysis must be route or time");
    auto path = std::filesystem::u8path(options.get("--file"));
    const auto bytes = std::filesystem::file_size(path), start = ticks();
    const auto summary = analyze(path, mode == "route" ? Analysis::Route : Analysis::Time, bytes);
    const auto finish = ticks(), duration = static_cast<std::uint64_t>((finish - start) * 1000000.0 / frequency());
    const auto timing_text = options.get("--timing-file");
    if (!timing_text.empty()) {
        const auto timing_path = std::filesystem::u8path(timing_text);
        if (!timing_path.parent_path().empty())
            std::filesystem::create_directories(timing_path.parent_path());
        std::ofstream timing(timing_path, std::ios::trunc);
        if (!timing)
            throw std::runtime_error("Cannot open timing-file");
        timing << "{\"start_ticks\":" << start << ",\"finish_ticks\":" << finish << ",\"qpc_frequency\":" << frequency()
               << ",\"duration_us\":" << duration << ",\"bytes\":" << bytes << ",\"valid_records\":" << summary.valid
               << ",\"malformed_records\":" << summary.malformed << "}\n";
        timing.flush();
        if (!timing)
            throw std::runtime_error("Cannot write timing-file");
    }
    std::cout << summary_json(summary) << std::endl;
    return 0;
} catch (const std::exception &e) {
    std::cerr << "reference: " << e.what() << std::endl;
    return 1;
}
