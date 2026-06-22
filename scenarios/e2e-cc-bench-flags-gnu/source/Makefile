# Same bench-observed codegen flags as e2e-cc-bench-flags, but driven through
# GCC instead of clang — the proof that kache's `gcc -###` parsing (cc1/cc1plus
# line) captures CapturedByProbe codegen flags so they cache on gcc, not just
# clang. This is exactly the compiler the LLVM nightly bench used (/usr/bin/cc),
# where -fno-semantic-interposition refused all 2279 TUs.
#
# CC is overridden by the harness to "$KACHE gcc". gcc-portable flags only:
# -mrecip= is x86-only (the bench/runner is also aarch64), so it is omitted here;
# -fno-semantic-interposition is the load-bearing one for the LLVM bench.
CC     ?= cc
BUILD  := build
OBJ    := $(BUILD)/a.o
BIN    := $(BUILD)/app

CFLAGS := -O2 -ffast-math -fno-finite-math-only -fno-lto -fno-semantic-interposition

.PHONY: all clean
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): a.c | $(BUILD)
	$(CC) $(CFLAGS) -c a.c -o $(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -o $(BIN)

clean:
	rm -rf $(BUILD)
