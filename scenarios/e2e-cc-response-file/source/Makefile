# Response-file compile: `cc @args.rsp` puts the real compile flags
# (including the -c source/output) inside args.rsp. kache cannot see
# through a response file at classification time, so `CcArgs::refuse_reasons`
# refuses it (src/compiler/cc.rs) and the invocation passes through to the
# real compiler, which expands the file and compiles. Nothing is cached.
#
# Response files are how large Windows/cross builds stay under the command
# line length limit, so this passthrough contract is load-bearing.

CC     ?= cc
BUILD  := build
BIN    := $(BUILD)/main

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/main.o: src/main.c args.rsp | $(BUILD)
	$(CC) @args.rsp

$(BIN): $(BUILD)/main.o
	$(CC) $(BUILD)/main.o -o $(BIN)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)
