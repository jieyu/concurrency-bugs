# Makefile for the StringBuffer bug

CXX=g++
CXXFLAGS=-g
LDFLAGS=-lpthread

all: main

main: stringbuffer.o main.cpp
	$(CXX) $(CXXFLAGS) -o main main.cpp stringbuffer.o $(LDFLAGS)

stringbuffer.o: stringbuffer.cpp stringbuffer.hpp
	$(CXX) $(CXXFLAGS) -c -o stringbuffer.o stringbuffer.cpp

clean:
	rm -f stringbuffer.o main
