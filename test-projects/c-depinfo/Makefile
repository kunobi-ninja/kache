# C fixture that emits a dep-info sidecar next to the object.

CC     ?= cc
CFLAGS ?= -O0 -g
SRC    := src/foo.c
HEADERS := src/bar.h
BUILD  := build
OBJ    := $(BUILD)/foo.o
DEP    := $(BUILD)/foo.d
BIN    := $(BUILD)/foo

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): $(SRC) $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -MMD -MP -Isrc -c $(SRC) -o $(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -o $(BIN)

-include $(DEP)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)

