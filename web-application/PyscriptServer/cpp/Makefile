CC = g++
CXXFLAGS = -Wall -O2
BACKFLAGS = -lpthread -lstdc++fs -std=c++1z 
#INCPATH=-I/usr/local/src/mysql-5.7.36/include
INCPATH=-I/usr/local/mysql/include
LIBS=-L/usr/lib64/mysql -lcrypt -lmysqlclient
OBJS = main.o query.o
TARGET = main

%.o : %.cpp 
		$(CC) $(CXXFLAGS) -c $< $(BACKFLAGS) $(INCPATH) $(LIBS)

$(TARGET) : $(OBJS)
		$(CC) $(CXXFLAGS) $(OBJS) -o $(TARGET) $(BACKFLAGS) $(INCPATH) $(LIBS)

.PHONY: clean
clean:
	rm -f $(OBJS) $(TARGET)

-include $(OBJS:.o=.d)
