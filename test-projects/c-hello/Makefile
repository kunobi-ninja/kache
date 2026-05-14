# Minimal C fixture for kache cc-family wrapper e2e tests.
#
# Uses CC=cc by default; the e2e script overrides with `CC="$KACHE cc"`
# (or gcc / clang) to route through kache. Two-step build (compile + link)
# exercises both modes the wrapper has to support.

CC     ?= cc
CFLAGS ?= -O0 -g
SRC    := src/foo.c
HEADERS := src/bar.h
BUILD  := build
OBJ    := $(BUILD)/foo.o
BIN    := $(BUILD)/foo

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): $(SRC) $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -Isrc -c $(SRC) -o $(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -o $(BIN)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)
