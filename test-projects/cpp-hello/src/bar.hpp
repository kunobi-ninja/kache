#pragma once
#include <string_view>

namespace bar {
inline std::string_view greeting() noexcept {
    return "hello from bar";
}
} // namespace bar
