#include "chaosproxy/common/logging.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace chaosproxy {

Status InitLogging(const LoggingOptions& options) {
    if (options.logger_name.empty()) {
        return Status(StatusCode::kInvalidArgument,
                      "logger_name must not be empty");
    }
    if (options.queue_size == 0) {
        return Status(StatusCode::kInvalidArgument,
                      "logging queue_size must be greater than zero");
    }
    if (!options.enable_console && options.file_path.empty()) {
        return Status(StatusCode::kInvalidArgument,
                      "logging requires console or file sink");
    }

    try {
        spdlog::shutdown();
        spdlog::init_thread_pool(options.queue_size, 1);

        std::vector<spdlog::sink_ptr> sinks;
        if (options.enable_console) {
            sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
        }
        if (!options.file_path.empty()) {
            const auto parent = options.file_path.parent_path();
            if (!parent.empty()) {
                std::filesystem::create_directories(parent);
            }
            sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                options.file_path.string(), true));
        }

        auto logger = std::make_shared<spdlog::async_logger>(
            options.logger_name,
            sinks.begin(),
            sinks.end(),
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::overrun_oldest);
        logger->set_level(spdlog::level::info);
        logger->flush_on(spdlog::level::err);
        spdlog::set_default_logger(std::move(logger));
        return Status::Ok();
    } catch (const std::exception& error) {
        return Status(StatusCode::kInternal,
                      std::string("failed to initialize logging: ") + error.what());
    }
}

}  // namespace chaosproxy
