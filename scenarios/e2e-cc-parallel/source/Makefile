# Many small TUs compiled in PARALLEL through one shared kache cache. The
# harness runs `make -j`, so a dozen `$KACHE cc -c` wrappers hit the store
# concurrently — exercising parallel lookups, stores, and (on warm) restores
# that a serial single-TU fixture never touches. CC is overridden by the
# harness to "$KACHE cc".
CC     ?= cc
CFLAGS ?= -O2 -g
BUILD  := build
SRCS   := $(wildcard src/*.c)
OBJS   := $(patsubst src/%.c,$(BUILD)/%.o,$(SRCS))
BIN    := $(BUILD)/app

.PHONY: all clean
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/%.o: src/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN): $(OBJS)
	$(CC) $(OBJS) -o $(BIN)

clean:
	rm -rf $(BUILD)
