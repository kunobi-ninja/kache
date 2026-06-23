cmake_minimum_required(VERSION 3.13)
project(ninja_flagset CXX)

# A header generated INTO the build tree and included via the build dir (-I) —
# the LLVM TableGen/config-header shape. The realistic Release flag set is set
# from scenario.toml via CMAKE_CXX_FLAGS.
configure_file(version.h.in version.h @ONLY)

add_executable(app src/main.cpp)
target_include_directories(app PRIVATE "${CMAKE_CURRENT_BINARY_DIR}" src)
