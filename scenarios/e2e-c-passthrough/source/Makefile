# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-ffast-math` — an unmodeled codegen flag
# that kache's classifier does not recognize. The compile passes
# through to the real compiler, caching nothing.
#
# Choice of flag: this needs an "unmodeled codegen flag that won't
# get classified later". `-march=native` was originally used for this
# but #115 added `Prefix("-march=")` to the cc table (the resolved
# `cc -###` tokens differentiate the per-host target-cpu / target-
# feature set, so `-march=*` is cacheable per-machine). `-ffast-math`
# changes float semantics in ways the modeled cache-key inputs don't
# capture today; if a future PR teaches the cache key about float
# modes, that PR should pick a different fixture flag — the docstring
# in the corresponding row of `CC_FLAGS` should mention this one.

CC      ?= cc
CFLAGS  ?= -O2 -ffast-math
SRC     := src/main.c
BUILD   := build
OBJ     := $(BUILD)/main.o
BIN     := $(BUILD)/main

.PHONY: all clean
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): $(SRC) | $(BUILD)
	$(CC) $(CFLAGS) -c $(SRC) -o $(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -o $(BIN)

clean:
	rm -rf $(BUILD)
