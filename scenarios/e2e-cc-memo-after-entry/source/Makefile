# One source compiled twice, once with a diagnostics-only flag. The objects
# are identical, so both outputs share one cache entry, but the flag is part
# of the preprocessor memo key, so each compile has its own memo. CC is
# overridden by the harness to "$KACHE cc".
CC     ?= cc
CFLAGS ?= -O0 -g
BUILD  := build

.PHONY: all clean
all: $(BUILD)/tool

$(BUILD)/a/util.o: src/util.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/b/util.o: src/util.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -fdiagnostics-color=never -c $< -o $@

# Link only one copy; the other is compiled for the cache's sake.
$(BUILD)/tool: $(BUILD)/a/util.o $(BUILD)/b/util.o src/main.c
	$(CC) $(CFLAGS) -c src/main.c -o $(BUILD)/main.o
	$(CC) $(BUILD)/main.o $(BUILD)/a/util.o -o $@

clean:
	rm -rf $(BUILD)
