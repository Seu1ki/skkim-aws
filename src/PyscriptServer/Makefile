CC = g++
CXXFLAGS = -Wall -O2
BACKFLAGS = -lpthread -lstdc++fs -std=c++1z 
OBJS = main.o query.o
TARGET = main

%.o : %.cpp 
		$(CC) $(CXXFLAGS) -c $< $(BACKFLAGS)

$(TARGET) : $(OBJS)
		$(CC) $(CXXFLAGS) $(OBJS) -o $(TARGET) $(BACKFLAGS)

.PHONY: clean
clean:
	rm -f $(OBJS) $(TARGET)

-include $(OBJS:.o=.d)
