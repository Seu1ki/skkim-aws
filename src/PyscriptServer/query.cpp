#include "query.h"

//#include "/usr/include/mysql/mysql.h"
//#include "/usr/include/mysql/my_global.h"
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
	std::cout<<"Hello"<<std::endl;
	MYSQL* conn, connection;
	MYSQL_RES* result;
	MYSQL_ROW row;

	char DB_HOST[] = "skkim-db.cshvzopeiwd9.ap-northeast-2.rds.amazonaws.com";
	char DB_USER[] = "admin";
	char DB_PASS[] = "intern19";
	char DB_NAME[] = "skkim_db";
	char sql[1024];
	char str1[1024], str2[1024];

	mysql_init(&connection);
	conn = mysql_real_connect(&connection, DB_HOST, DB_USER, DB_PASS, DB_NAME, 3306, (char *)NULL, 0);

	/*

	strcpy(sql, "SELECT * FROM accum_sum");
	if(mysql_query(conn, sql) == 0){
		result = mysql_store_result(conn);
		while((row = mysql_fetch_row(result)) != NULL){
			strcpy(str1, row[0]);
			strcpy(str2, row[0]);
		}
		mysql_free_result(result);
	}
	else{
		std::cout << "query error" << std::endl;
		// 에러
	}

	mysql_close(conn);
	*/
}

