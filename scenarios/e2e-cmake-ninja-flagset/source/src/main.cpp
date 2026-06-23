#include "version.h"
#include <cstdio>

// A little non-trivial code so the realistic codegen flags (sections,
// frame-pointer, fast-math, visibility) have something to act on. No
// exceptions / RTTI usage, so -fno-exceptions / -fno-rtti compile clean.
namespace {
double accumulate(const double* xs, int n) {
    double s = 0.0;
    for (int i = 0; i < n; ++i) s += xs[i] * xs[i];
    return s;
}
}  // namespace

int main() {
    const double xs[] = {1.5, 2.5, 3.5};
    std::printf("%s ok (%.1f)\n", BUILD_TAG, accumulate(xs, 3));
    return 0;
}
