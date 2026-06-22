# Firefox-on-Windows clang-cl flag set (issue #411), driven on any OS via
# `clang --driver-mode=cl`. CC is overridden by the e2e harness to
# "$KACHE clang --driver-mode=cl". Compile-only (-c, no link) so it needs
# no MSVC SDK; a.c has no #includes so it needs no headers.
CC    ?= clang --driver-mode=cl
BUILD := build
OBJ   := $(BUILD)/a.obj
DEP   := $(BUILD)/a.pp

.PHONY: all clean
all: $(OBJ)

$(BUILD):
	mkdir -p $(BUILD)

# The flag set that issue #411 reported as unsupported. `-Xclang <tok>`
# forwards each cc1 dep-info / diagnostics token to the front end.
$(OBJ): a.c | $(BUILD)
	$(CC) -c -TP -ffile-reproducible \
	  -Xclang -MP -Xclang -dependency-file -Xclang $(DEP) \
	  -Xclang -MT -Xclang $(OBJ) \
	  -Xclang -fansi-escape-codes \
	  -Fo$(OBJ) a.c

clean:
	rm -rf $(BUILD)
