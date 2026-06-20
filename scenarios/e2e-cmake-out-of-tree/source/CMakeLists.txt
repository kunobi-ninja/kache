cmake_minimum_required(VERSION 3.13)
project(oot_c C)

# A header generated INTO the build tree and included via the build dir (-I).
# This is the cross-variant generated-source case from kunobi-ninja/kache#304:
# the compile carries an absolute -I into the out-of-tree build dir, which must
# normalize so a relocated build still converges.
configure_file(version.h.in version.h @ONLY)

add_executable(foo src/foo.c)
target_include_directories(foo PRIVATE "${CMAKE_CURRENT_BINARY_DIR}" src)
