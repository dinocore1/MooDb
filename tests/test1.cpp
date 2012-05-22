/*
 * test1.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#include <iostream>
#include <moodb/moodb.h>
#include "Poco/Logger.h"
#include "Poco/ConsoleChannel.h"

using Poco::Logger;
using Poco::ConsoleChannel;

int main(int argv, const char* argc) {
	//setup logging
	Logger::root().setChannel(new ConsoleChannel());

	moodb* db;
	moodb_open("test1.db", &db);

	//execute_update(db, "PRAGMA synchronous = OFF;");

	moodb_putobject(db, "guy", "({ firstname: \"John\", lastname: \"Doe\", age:50, eyecolor: \"blue\" })", 0);
	moodb_putobject(db, "guy", "({ firstname: \"Paul\", lastname: \"Soucy\", age:27, eyecolor: \"green\" })", 0);

	char *data;
	moodb_getobject(db, "guy", &data);
	std::cout << "guy value: " << data << std::endl;
	moodb_free(data);

	moodb_putobject(db, 0, "({ firstname: \"Melanie\", lastname: \"Silverman\", age:25, eyecolor: \"brown\" })", 0);
	moodb_putobject(db, 0, "({ firstname: \"Ben\", lastname: \"Silverman\", age:29, eyecolor: \"brown\" })", 0);
	moodb_putobject(db, 0, "({ firstname: \"Gabe\", lastname: \"Silverman\", eyecolor: \"brown\" })", 0);

	moodb_putview(db, "({name: \"people\", map: function(obj) { if(obj.eyecolor){emit(\"eyecolor\", obj.eyecolor);} "
			"if(obj.age){ emit(\"age\", obj.age);} } })");

	moocursor *cursor;
	moodb_query(db, &cursor, "({view: \"m_people\", filter: \"age <> 25\" })");

	std::cout << std::endl << "Query:" << std::endl;

	char *key, *value;
	while(moodbcursor_next(cursor, &key, &value) == MOODB_OK){
		std::cout << key << " : " << value << std::endl;
	}
	moodbcursor_close(cursor);


	moodb_close(db);
}
