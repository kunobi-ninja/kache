# Two tools that each vendor a byte-identical copy of the same helper
# (src/a and src/b). The copies read the same bytes but are different
# translation units: each one's object spells its own path (`__FILE__`, the
# debug info's file name). CC is overridden by the harness to "$KACHE cc".
CC     ?= cc
CFLAGS ?= -O0 -g
BUILD  := build

.PHONY: all clean
# Keep the objects: the scenario diffs them, and noop expects them.
.SECONDARY:
all: $(BUILD)/tool-a $(BUILD)/tool-b

$(BUILD)/%.o: src/%.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD)/tool-%: $(BUILD)/main.o $(BUILD)/%/util.o
	$(CC) $^ -o $@

clean:
	rm -rf $(BUILD)
