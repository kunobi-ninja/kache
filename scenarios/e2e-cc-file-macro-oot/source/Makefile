# Out-of-tree C++ build for the #410 __FILE__-normalization reproducer.
#
# The compile runs from a SIBLING build dir (../oot-build) with the source passed
# as an absolute path, so kache's push_cwd_source_roots takes the sibling branch
# and injects an -ffile-prefix-map into the real compile — the exact topology that
# flattened __FILE__ to "<CC_SOURCE>/location.cc" and failed the cold Firefox build.
#
# CXX defaults to plain c++; the harness overrides it with `$KACHE c++` to route
# through kache. KACHE_BASE_DIR is the per-clone source root (the documented knob
# the real Firefox/LLVM benches use): it normalizes the working-dir basename out of
# __FILE__ so a relocated/cross-clone build converges. No -g: DWARF path baking is a
# separate concern.
CXX      ?= c++
CXXFLAGS ?= -std=c++17 -O0
SRCDIR   := $(CURDIR)
BUILD    := $(CURDIR)/../oot-build
SRC      := $(SRCDIR)/src/base/location.cc

.PHONY: all clean run
all:
	mkdir -p $(BUILD)
	cd $(BUILD) && KACHE_BASE_DIR=$(SRCDIR) $(CXX) $(CXXFLAGS) -c $(SRC) -o location.o
	cd $(BUILD) && $(CXX) location.o -o location

run:
	$(BUILD)/location

clean:
	rm -rf $(BUILD)
