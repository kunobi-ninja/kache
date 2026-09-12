# C and assembler build for the kunobi-ninja/kache#1015 reproducer.
#
# blob.S and inline.c embed build/payload.txt with `.incbin`. make writes a
# checksum of this checkout's path into it, so the payload differs between
# checkouts while blob.S and inline.c do not, and no object carries a path.
# `.incbin` files never reach a depfile, so the cc key cannot see the payload.
# main.c gets the same checksum as EXPECTED and checks both payloads at runtime.
#
# CC defaults to plain cc; the harness overrides it with `$KACHE cc`.
CC     ?= cc
CFLAGS ?= -O0
BUILD  := $(CURDIR)/build
CHECK  := $(shell printf '%s' '$(CURDIR)' | cksum | cut -d' ' -f1)

.PHONY: all clean
all:
	mkdir -p $(BUILD)
	printf '%s' '$(CHECK)' > $(BUILD)/payload.txt
	cd $(BUILD) && $(CC) $(CFLAGS) -c $(CURDIR)/src/blob.S -o blob.o
	cd $(BUILD) && $(CC) $(CFLAGS) -c $(CURDIR)/src/inline.c -o inline.o
	cd $(BUILD) && $(CC) $(CFLAGS) '-DEXPECTED="$(CHECK)"' -c $(CURDIR)/src/main.c -o main.o
	cd $(BUILD) && $(CC) blob.o inline.o main.o -o app

clean:
	rm -rf $(BUILD)
