#include "query.h"

#include <thread>
#include <iostream>
#include<getopt.h>

int main(int argc, char* argv[])
{
	
	int interval = 1000;

	Query tester;

	std::thread t{&Query::initTimer, &tester, interval, &Query::getInfo};
	t.detach();

	while(1);    
	return 0;
}
