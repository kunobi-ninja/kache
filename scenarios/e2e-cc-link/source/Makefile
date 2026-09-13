CC     ?= cc
CFLAGS ?= -O0
BUILD  := build
A      := $(BUILD)/a.o
B      := $(BUILD)/b.o
BIN    := $(BUILD)/prog

.PHONY: all clean
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(A): src/a.c | $(BUILD)
	$(CC) $(CFLAGS) -c src/a.c -o $(A)

$(B): src/b.c | $(BUILD)
	$(CC) $(CFLAGS) -c src/b.c -o $(B)

$(BIN): $(A) $(B)
	$(CC) $(A) $(B) -o $(BIN)

clean:
	rm -rf $(BUILD)
