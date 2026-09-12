# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-mtune=skylake` — an unmodeled codegen flag
# that kache's classifier does not recognize. The compile passes
# through to the real compiler, caching nothing.
#
# Choice of flag: this needs an "unmodeled codegen flag that won't
# get classified later". `-march=native` was the original pick but
# #115 added `Prefix("-march=")`. `-ffast-math` then `-funroll-loops`
# followed; both are now `CapturedByProbe` after nightlies showed
# them on real TUs. `-mtune=skylake` is the current pick; if a future
# PR models it, that PR should pick a different fixture flag here.

CC      ?= cc
CFLAGS  ?= -O2 -mtune=skylake
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
