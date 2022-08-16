#include "query.h"

#include <mysql.h>
#include <my_global.h>

#include <stdio.h>
#include <string.h>

#include <future>
#include <chrono>
#include <experimental/filesystem>
#include <fstream>
#include <sstream>

#include <iostream>


void Query::initTimer(int militime, void (*f)())
{
	while(true)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(militime));
		f();
	}
}

void Query::getInfo()
{
	MYSQL* conn, connection;
	MYSQL_RES* result;
	MYSQL_ROW row;

	char DB_HOST[] = MYSQL_ENDPOINT;
	char DB_USER[] = MYSQL_USER;
	char DB_PASS[] = MYSQL_PASSWORD;
	char DB_NAME[] = MYSQL_DATABASE;
	char sql[1024];


	std::fstream fout;


	mysql_init(&connection);
	conn = mysql_real_connect(&connection, DB_HOST, DB_USER, DB_PASS, DB_NAME, 3306, (char *)NULL, 0);


	strcpy(sql, "SELECT * FROM accum_sum WHERE weather LIKE 'rain' LIMIT 7");
	mysql_query(conn, "set names euc-kr");
	if(mysql_query(conn, sql) == 0){
		result = mysql_store_result(conn);
		row = mysql_fetch_row(result);

		fout.open("accum_sum.csv", std::ios::out );
			while((row = mysql_fetch_row(result)) != NULL){
				printf("%s %s, %s %s %s %s %s %s \n", row[0], row[1],row[2],row[3],row[4],row[5],row[6],row[7]);
				fout << row[0] << ","
				       << row[1] << ","
				       << row[2] << ","
				       << row[3] << ","
				       << row[4] << ","
				       << row[5] << ","
				       << row[6] << ","
				       << row[7] << "\n";
			}
		mysql_free_result(result);
		fout.close();
	}
	else{
		std::cout << "query error" << std::endl;
	}

	mysql_close(conn);
}

