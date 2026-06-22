# Firefox media-TU math/codegen flag set (nightly bench passthrough canary).
#
# CC is overridden by the harness to "$KACHE clang". These flags are
# CapturedByProbe: kache parses clang's resolved `cc -###` `-cc1` tokens, which
# fan -ffast-math out into -ffinite-math-only/-ffp-contract=fast/-menable-no-infs/
# … and forward -mrecip=/-fno-finite-math-only, so the cache key captures their
# codegen effect. (kache parses -### for clang, not gcc, so this fixture pins
# clang — mirrors e2e-cc-cl-xclang-deps.) -fno-lto is a no-op for a -c compile.
#
# Before they were modeled, the real Firefox build passed these TUs through
# uncached ("cc unsupported flag(s): -ffast-math -mrecip=none
# -fno-finite-math-only"). The miss-then-hit contract IS the no-passthrough
# assertion: if any flag refuses, cold stores nothing and warm can't hit.
CC     ?= cc
BUILD  := build
OBJ    := $(BUILD)/a.o
BIN    := $(BUILD)/app

# The exact families the nightly benches passed through: Firefox media TUs'
# fast-math set + negation knob, and -fno-semantic-interposition — the single
# flag LLVM's Release build puts on EVERY TU that refused all 2279 LLVM compiles
# (0% hit rate) until modeled.
CFLAGS := -O2 -ffast-math -fno-finite-math-only -mrecip=none -fno-lto -fno-semantic-interposition

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
