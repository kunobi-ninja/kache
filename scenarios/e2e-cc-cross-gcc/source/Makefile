RISCV ?= riscv64-linux-gnu-gcc
ARM   ?= arm-linux-gnueabihf-gcc
KACHE ?=
BUILD := build

.PHONY: all clean
all: $(BUILD)/a-lp64.o $(BUILD)/a-lp64d.o $(BUILD)/a-arm.o

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/a-lp64.o: src/a.c | $(BUILD)
	$(KACHE) $(RISCV) -O0 -c -march=rv64gc -mabi=lp64 src/a.c -o $@

$(BUILD)/a-lp64d.o: src/a.c | $(BUILD)
	$(KACHE) $(RISCV) -O0 -c -march=rv64gc -mabi=lp64d src/a.c -o $@

$(BUILD)/a-arm.o: src/a.c | $(BUILD)
	$(KACHE) $(ARM) -O0 -c -march=armv7-a -mthumb -mfpu=vfpv3-d16 -mfloat-abi=hard src/a.c -o $@

clean:
	rm -rf $(BUILD)
