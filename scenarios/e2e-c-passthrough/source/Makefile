# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-funroll-loops` — an unmodeled codegen flag
# that kache's classifier does not recognize. The compile passes
# through to the real compiler, caching nothing.
#
# Choice of flag: this needs an "unmodeled codegen flag that won't
# get classified later". `-march=native` was the original pick but
# #115 added `Prefix("-march=")` (the resolved `cc -###` tokens
# differentiate the per-host target-cpu/feature set). `-ffast-math`
# was the next pick, but the Firefox nightly bench showed it forcing
# real TUs through uncached, so it is now `CapturedByProbe` too.
# `-funroll-loops` is the current pick — a genuine codegen
# optimization the classifier still refuses; if a future PR models it,
# that PR should pick a different fixture flag here.

CC      ?= cc
CFLAGS  ?= -O2 -funroll-loops
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
