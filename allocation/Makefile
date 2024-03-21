CXXFLAGS = -Wall -Wextra -std=c++17 -g
LDFLAGS = -lpthread

SRCS = main.cc binaryalloc.cc chunkalloc.cc
OBJS = $(SRCS:.cc=.o)
MAIN = main

.PHONY: all clean

all: $(MAIN)

$(MAIN): $(OBJS)
	$(CXX) $(CXXFLAGS) $(OBJS) -o $(MAIN) $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	$(RM) $(OBJS) $(MAIN)
