all: runtest

CXXFLAGS ?= -O3 -g -Wall -Wextra -Werror

runtest: test
	@rm -f binary.bin
	@clear
	@time ./test 25000 && ls -l binary.bin

test: test.cpp BinaryFile.hpp
	$(CXX) $(CXXFLAGS) -o $@ $^ -I. -lz

clean:
	rm -f binary.bin test

