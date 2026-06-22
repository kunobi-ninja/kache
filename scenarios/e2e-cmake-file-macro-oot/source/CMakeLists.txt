cmake_minimum_required(VERSION 3.13)
project(oot_file_macro CXX)

# A single C++ TU one directory deep (src/base/location.cc). Built OUT OF TREE
# (a sibling build dir, see scenario.toml) with kache as CMAKE_CXX_COMPILER_LAUNCHER
# — the LLVM-bench build shape. kache takes the sibling branch of
# push_cwd_source_roots and injects an -ffile-prefix-map into the real compile.
# The buggy map flattened __FILE__ to "<CC_SOURCE>/location.cc"; the fixed one
# folds <CC_ROOT> and keeps "…/base/location.cc", which the source static_asserts.
add_executable(location src/base/location.cc)
