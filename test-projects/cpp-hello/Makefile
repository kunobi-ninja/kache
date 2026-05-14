# Minimal C++ fixture for kache cc-family wrapper e2e tests.
#
# Uses CXX=c++ by default; the e2e script overrides with `CXX="$KACHE c++"`
# (or g++ / clang++) to route through kache. Includes a template + STL pull
# to stress real-world preprocessor expansion against system headers.

CXX      ?= c++
CXXFLAGS ?= -std=c++17 -O0 -g
SRC      := src/foo.cpp
HEADERS  := src/bar.hpp
BUILD    := build
OBJ      := $(BUILD)/foo.o
BIN      := $(BUILD)/foo

.PHONY: all clean run
all: $(BIN)

$(BUILD):
	mkdir -p $(BUILD)

$(OBJ): $(SRC) $(HEADERS) | $(BUILD)
	$(CXX) $(CXXFLAGS) -Isrc -c $(SRC) -o $(OBJ)

$(BIN): $(OBJ)
	$(CXX) $(OBJ) -o $(BIN)

run: $(BIN)
	$(BIN)

clean:
	rm -rf $(BUILD)
