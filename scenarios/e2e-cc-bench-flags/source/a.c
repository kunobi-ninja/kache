#include <stdio.h>

// A little floating-point work so -ffast-math / -mrecip / -ffinite-math-only
// actually have codegen to act on. The printed string is what the harness
// checks; the math just gives the math flags something to transform.
double accumulate(const double *xs, int n) {
    double s = 0.0;
    for (int i = 0; i < n; i++) {
        s += xs[i] * xs[i];
    }
    return s;
}

int main(void) {
    double xs[] = {1.5, 2.5, 3.5, 4.5};
    double r = accumulate(xs, 4);
    printf("math-flags ok (%.1f)\n", r);
    return 0;
}
