# C build for the kunobi-ninja/kache#1004 reproducer.
#
# data.c returns DATA_DIR, an absolute path under this checkout passed with -D.
# -ffile-prefix-map rewrites __FILE__ and debug info, not string literals, so
# data.o carries the path verbatim. main.c embeds no path and stays portable.
#
# CC defaults to plain cc; the harness overrides it with `$KACHE cc`.
CC     ?= cc
CFLAGS ?= -O0
BUILD  := $(CURDIR)/build

.PHONY: all clean
all:
	mkdir -p $(BUILD)
	cd $(BUILD) && $(CC) $(CFLAGS) '-DDATA_DIR="$(CURDIR)/data"' -c $(CURDIR)/src/data.c -o data.o
	cd $(BUILD) && $(CC) $(CFLAGS) -c $(CURDIR)/src/main.c -o main.o
	cd $(BUILD) && $(CC) data.o main.o -o app

clean:
	rm -rf $(BUILD)
