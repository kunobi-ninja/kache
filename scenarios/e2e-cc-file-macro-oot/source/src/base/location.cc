// Minimal reproducer for the cold Firefox bench failure (kunobi-ninja/kache#410):
// Chromium's base/location.cc static_asserts that __FILE__ keeps its trailing
// directory component ("base/location.cc"). kache rewrites the source path in the
// REAL compile via -ffile-prefix-map (it injects the same map it folds into the
// cache key). For a sibling out-of-tree build, the buggy normalization mapped the
// source file's IMMEDIATE parent to <CC_SOURCE>, which wins under longest-match and
// flattens __FILE__ to "<CC_SOURCE>/location.cc" — dropping the "base/" component
// and failing this assert at COLD compile, before any cache hit is even possible.
//
// Folding the shared <CC_ROOT> instead preserves "…/base/location.cc", so the
// assert holds while the key still normalizes across clones (see the relocate
// phase in scenario.toml). This file mirrors Chromium's compile-time check so the
// regression is caught at gate speed instead of only by the hours-long bench.
#include <cstddef>
#include <iostream>

namespace {
constexpr std::size_t StrLen(const char* s) {
    std::size_t n = 0;
    while (s[n] != '\0') {
        ++n;
    }
    return n;
}

// True iff `name` ends with `suffix`. Mirrors Chromium base/location.cc.
constexpr bool StrEndsWith(const char* name, const char* suffix) {
    const std::size_t name_len = StrLen(name);
    const std::size_t suffix_len = StrLen(suffix);
    if (name_len < suffix_len) {
        return false;
    }
    const char* tail = name + (name_len - suffix_len);
    for (std::size_t i = 0; i < suffix_len; ++i) {
        if (tail[i] != suffix[i]) {
            return false;
        }
    }
    return true;
}

// The crux: __FILE__ must retain its directory component after kache's
// path normalization. A flattened "<CC_SOURCE>/location.cc" fails here.
static_assert(StrEndsWith(__FILE__, "base/location.cc"),
              "__FILE__ lost its 'base/' directory component — kache normalization "
              "flattened the source path. See kunobi-ninja/kache#410.");
}  // namespace

int main() {
    std::cout << "location ok\n";
    return 0;
}
