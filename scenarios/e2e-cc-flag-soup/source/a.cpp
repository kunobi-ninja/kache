#include <cstdio>

// Trivial TU; the point of this fixture is the COMPILER FLAGS, not the code.
int compute() { return 42; }

int main() {
    std::printf("flag-soup ok: %d\n", compute());
    return 0;
}
