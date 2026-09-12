# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-Ofast` — an unmodeled codegen flag
# that kache's classifier does not recognize. The compile passes
# through to the real compiler, caching nothing.
#
# Choice of flag: this needs an "unmodeled codegen flag that compiles
# on every CI OS". `-march=native` then `-ffast-math` then
# `-funroll-loops` were classified from nightlies. `-mtune=skylake`
# compiles on Linux but Apple clang on arm64 rejects the CPU name.
# `-Ofast` is the current pick; if a future PR models it, that PR
# should pick a different fixture flag here.

CC      ?= cc
CFLAGS  ?= -Ofast
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
