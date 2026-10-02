# Two byte-identical sources at different paths, compiled by clang-cl and
# driven on any OS via `clang --driver-mode=cl`. CC is overridden by the e2e
# harness to "$KACHE clang --driver-mode=cl". Compile-only (-c, no link) and
# no system headers, so it needs no MSVC SDK.
CC    ?= clang --driver-mode=cl
BUILD := build

.PHONY: all clean
all: $(BUILD)/a/util.obj $(BUILD)/b/util.obj

# -Brepro zeroes the COFF timestamp, so a fresh compile is comparable byte
# for byte with a restored one.
$(BUILD)/%.obj: src/%.c
	mkdir -p $(dir $@)
	$(CC) -c -Brepro -Fo$@ $<

clean:
	rm -rf $(BUILD)
