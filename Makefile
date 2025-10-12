CXX ?= clang++
CXXFLAGS ?= -O2 -std=c++17
BIN = build/edgar_sessionization

all: $(BIN)

$(BIN): src/edgar_sessionization.cpp
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -o $(BIN) $<

run: all
	@mkdir -p output
	$(BIN) input/log.csv input/inactivity_period.txt output/sessionization.txt
	@echo "Wrote output/sessionization.txt"

clean:
	rm -rf build
