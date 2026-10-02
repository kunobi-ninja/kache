# One source compiled twice, through two spellings of the same compiler.
# The objects are identical, so both outputs share one cache entry, but
# each spelling has its own preprocessor memo. The harness sets CC to
# "$KACHE cc" and CC_ALT to the same driver by absolute path.
CC     ?= cc
CC_ALT ?= $(CC)
CFLAGS ?= -O0 -g
BUILD  := build

.PHONY: all clean
all: $(BUILD)/tool

$(BUILD)/a/util.o: src/util.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/b/util.o: src/util.c
	mkdir -p $(dir $@)
	$(CC_ALT) $(CFLAGS) -c $< -o $@

# Link only one copy; the other is compiled for the cache's sake.
$(BUILD)/tool: $(BUILD)/a/util.o $(BUILD)/b/util.o src/main.c
	$(CC) $(CFLAGS) -c src/main.c -o $(BUILD)/main.o
	$(CC) $(BUILD)/main.o $(BUILD)/a/util.o -o $@

clean:
	rm -rf $(BUILD)
