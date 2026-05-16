# kache-e2e fixture: a C build kache must NOT cache.
#
# The `-c` compile passes `-march=native` — a flag whose result
# depends on the BUILD HOST's CPU. kache is a cross-machine,
# content-addressed cache, so it can never safely cache a
# host-specific object; `-march=native` is permanently uncacheable by
# design. kache detects it as an unmodeled `-m...` codegen flag and
# passes the compile through to the real compiler, caching nothing.
#
# `-march=native` is chosen deliberately over a flag like
# `-funroll-loops`: that one will eventually be classified as
# cacheable (hashed into the key), which would silently break this
# fixture's "caches nothing" contract. `-march=native` never will —
# so the fixture stays valid as the support matrix grows.

CC      ?= cc
CFLAGS  ?= -O2 -march=native
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
