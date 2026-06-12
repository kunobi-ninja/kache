# clang-cl debug-info fixture for kache (#312). CC is overridden by the
# e2e harness to "$KACHE clang-cl". /Z7 embeds CodeView in the .obj;
# /Brepro makes the object reproducible (box-confirmed) so a cache hit
# is byte-identical to a fresh compile.
CC    ?= clang-cl
BUILD := build
OBJ   := $(BUILD)/a.obj
BIN   := $(BUILD)/a.exe

all: $(BIN)

$(BUILD):
	mkdir $(BUILD)

$(OBJ): a.c | $(BUILD)
	$(CC) /Brepro /Z7 -c a.c -Fo$(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -Fe$(BIN)

clean:
	rm -rf $(BUILD)
