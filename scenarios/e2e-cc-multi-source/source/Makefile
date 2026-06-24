# Multi-source single-invocation compile: `cc -c a.c b.c main.c` builds
# three translation units in ONE process. kache's cc cache is per-TU, so
# it refuses this shape and passes through (see scenario.toml). Objects
# land in the build dir; a second invocation links them.

CC     ?= cc
CFLAGS ?= -O2
BUILD  := build
SRCS   := src/a.c src/b.c src/main.c
OBJS   := $(BUILD)/a.o $(BUILD)/b.o $(BUILD)/main.o
BIN    := $(BUILD)/main

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

# One multi-source compile invocation (no -o; -c emits one object per
# source). Objects are written into BUILD via -o is not allowed with
# multiple -c inputs, so compile in BUILD as the working dir.
$(OBJS): $(SRCS) | $(BUILD)
	cd $(BUILD) && $(CC) $(CFLAGS) -c ../src/a.c ../src/b.c ../src/main.c

$(BIN): $(OBJS)
	$(CC) $(OBJS) -o $(BIN)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)
