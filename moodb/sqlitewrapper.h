/*
 * sql.h
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#ifndef SQLITEWRAPPER_H_
#define SQLITEWRAPPER_H_

#include <sqlite3.h>

class SqliteWrapper;

class SqliteCursor {
	friend class SqliteWrapper;
public:
	SqliteCursor();
	SqliteCursor(sqlite3_stmt *stmt);
	~SqliteCursor();

	int step();

	int getInt(int column);
	long getLong(int column);
	const char* getText(int column);

private:
	sqlite3_stmt *pSqlStmt;
};

class SqliteTransactionScope {
	friend class SqliteWrapper;
public:
	SqliteTransactionScope();
	~SqliteTransactionScope();

	void setSuccesfull();

private:
	SqliteWrapper *mDb;
	bool mIsSuccess;
};

class SqliteWrapper {

public:
	~SqliteWrapper();

	int open(const char *filename);

	int execute_query(SqliteCursor *cursor, const char *sql, ...);
	int execure_query_va(SqliteCursor *cursor, const char *sql, va_list args);
	int execute_update(const char *sql, ...);

	void beginTransaction(SqliteTransactionScope *tr);

	const char* getLastError();
	sqlite3_int64 getLastInsertRow();

private:
	sqlite3 *pDB;

};




#endif /* SQLITEWRAPPER_H_ */
