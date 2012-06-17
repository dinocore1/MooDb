/*
 * test1.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#include <time.h>


#include <moodb/moodb.h>

int main(int argv, const char* argc) {
	moodb* db;
	time_t begin, end;
	int i;

	time(&begin);

	moodb_open("test1.db", &db);

	//execute_update(db, "PRAGMA synchronous = OFF;");

	for(i=0;i<1000;i++){
		moodb_putobject(db, 0, "{ firstname: \"John\", lastname: \"Doe\", age:50, eyecolor: \"blue\" }", 0);
	}

	//execute_update(db, "COMMIT TRANSACTION;");

	time(&end);

	printf("\nTime elapased: %.2lf seconds", difftime(end,begin));

	moodb_close(db);
}
