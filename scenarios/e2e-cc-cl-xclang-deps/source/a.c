/* Firefox-on-Windows clang-cl flag fixture for kache (issue #411).
 * No #includes on purpose: `clang --driver-mode=cl -c` then needs no MSVC
 * SDK, so the clang-cl flag-classification path runs on Linux/macOS CI. */
int answer(void) { return 42; }
