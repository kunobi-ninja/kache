# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-funroll-loops` — a codegen-affecting flag
# in the `-f...` family that kache does not model in its cache key.
# kache refuses such flags (passthrough) rather than risk a silent
# miscache, so every build here runs the real compiler and caches
# nothing. The two-step compile + link both pass through.

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
