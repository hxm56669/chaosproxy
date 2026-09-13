#pragma once

#include "chaosproxy/common/status_or.h"

#include <cstdint>
#include <string>

namespace phototask {

struct ImageData { std::string bytes; std::uint32_t width = 0; std::uint32_t height = 0; };

class ImageProcessors {
 public:
  static chaosproxy::StatusOr<ImageData> Thumbnail(const std::string& input,
                                                    std::uint32_t max_width,
                                                    std::uint32_t max_height);
  static chaosproxy::StatusOr<std::string> ExtractMetadata(const ImageData& image);
};

}  // namespace phototask
