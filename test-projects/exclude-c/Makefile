CC     ?= cc
CFLAGS ?= -O0 -g
BUILD  := build
OBJ    := $(BUILD)/foo.o
BIN    := $(BUILD)/foo

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): src/foo.c | $(BUILD)
	$(CC) $(CFLAGS) -c src/foo.c -o $(OBJ)

$(BIN): $(OBJ)
	$(CC) $(OBJ) -o $(BIN)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)
