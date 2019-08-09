all: 
	bear -a make main

main: main.cc
	g++ --std=c++11 -Wall -O2 $^ -o $@ -lrocksdb

format: main.cc
	clang-format -i $^
