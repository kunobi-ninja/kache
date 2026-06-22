# Realistic release-C++ flag set. CXX is overridden by the e2e harness to
# "$KACHE c++" (g++ on Linux, clang on macOS, clang-cl on Windows). Every
# flag here must be MODELED by kache's classifier so the compile caches on
# all three. Deliberately NO probe-captured codegen flags
# (-fstack-protector-strong, -fno-math-errno, -funwind-tables, -pthread, …):
# those need the resolved `cc -###` invocation, which kache parses for clang
# but not gcc, so they correctly pass through on Linux's g++ — a different
# contract (covered for clang by e2e-cc-cl-xclang-deps). This canary's job
# is "a representative MODELED set still caches"; if any of these regresses
# to unmodeled, cold stores nothing and the miss-then-hit contract fails.
CXX    ?= c++
BUILD  := build
OBJ    := $(BUILD)/a.o
DEP    := $(BUILD)/a.d
BIN    := $(BUILD)/app

# Modeled across gcc/clang/clang-cl: optimization + debug + PIC + std
# (ModeledInKey), warnings (NoObjectEffect), defines/includes
# (PreprocessorCaptured), dep-info sidecar (NoObjectEffect).
CXXFLAGS := -O2 -g -fPIC -std=c++17 -Wall -Wextra -Wno-unused-parameter \
	-DNDEBUG -DFLAG_SOUP=1 -I. -MMD -MP -MF $(DEP)

.PHONY: all clean
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): a.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c a.cpp -o $(OBJ)

$(BIN): $(OBJ)
	$(CXX) $(OBJ) -o $(BIN)

-include $(DEP)

clean:
	rm -rf $(BUILD)
