/*
 * sql.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */


#include "sqlitewrapper.h"
#include <cstddef>
#include <string>
#include <cstring>

using namespace std;

SqliteCursor::SqliteCursor() {
	pSqlStmt = NULL;
}

SqliteCursor::SqliteCursor(sqlite3_stmt *stmt) {
	pSqlStmt = stmt;
}

SqliteCursor::~SqliteCursor() {
	if(pSqlStmt != NULL){
		sqlite3_finalize(pSqlStmt);
	}
}

int SqliteCursor::step() {
	if(pSqlStmt != NULL){
		return sqlite3_step(pSqlStmt);
	}
	return 0;
}

int SqliteCursor::getInt(int column) {
	if(pSqlStmt != NULL){
		return sqlite3_column_int(pSqlStmt, column);
	}
}

long SqliteCursor::getLong(int column) {
	if(pSqlStmt != NULL) {
		return sqlite3_column_int64(pSqlStmt, column);
	}
}

const char* SqliteCursor::getText(int column) {
	if(pSqlStmt != NULL){
		return reinterpret_cast<const char*>( sqlite3_column_text(pSqlStmt, column) );
	}
}

SqliteTransactionScope::SqliteTransactionScope() {
	mDb = NULL;
	mIsSuccess = false;
}

SqliteTransactionScope::~SqliteTransactionScope() {
	if(mDb != NULL){
		if(mIsSuccess) {
			mDb->execute_update("COMMIT TRANSACTION;");
		} else {
			mDb->execute_update("ROLLBACK TRANSACTION;");
		}
	}
}

void SqliteTransactionScope::setSuccesfull() {
	mIsSuccess = true;
}

int SqliteWrapper::open(const char *filename){
	return sqlite3_open(filename, &pDB);
}

void SqliteWrapper::beginTransaction(SqliteTransactionScope *tr) {
	tr->mDb = this;
	tr->mIsSuccess = false;
	execute_update("BEGIN TRANSACTION;");
}

string translateSql(const string &sql){
	string retval;
	retval.reserve(sql.length() + 1);
	int i, z;
	z = 0;

	for(i=0;i<sql.length();i++){
		char theC = sql[i];
		if(theC == '@') {
			theC = '?';
			i++;
		}
		retval[z++] = theC;
	}
	retval[z++] = '\0';

	return retval;
}

int bindSql(const char *sql, sqlite3_stmt *pStmt, va_list args) {
	int i, z;
	z = 1;
	for(i=0;i<strlen(sql);i++){
		char theC = sql[i];
		if(theC == '@'){
			switch(sql[++i]){
			case 's': {
				char* thetext = va_arg(args, char*);
				if(sqlite3_bind_text(pStmt, z++, thetext, -1, SQLITE_STATIC) != SQLITE_OK){
					return SQLITE_ERROR;
				}
				break;
			}

			case 'l': {
				long value = va_arg(args, long);
				if(sqlite3_bind_int64(pStmt, z++, value) != SQLITE_OK){
					return SQLITE_ERROR;
				}
				break;
			}

			default:
				return SQLITE_ERROR;
			}
		}
	}

	return SQLITE_OK;
}

int SqliteWrapper::execure_query_va(SqliteCursor *cursor, const char *sql, va_list args) {
	int retval;

	string newSql = translateSql(sql);
	retval = sqlite3_prepare_v2(pDB, newSql.c_str(), -1, &cursor->pSqlStmt, 0);
	if(retval != SQLITE_OK) {
		return retval;
	}

	retval = bindSql(sql, cursor->pSqlStmt, args);


	return retval;
}

int SqliteWrapper::execute_query(SqliteCursor *cursor, const char *sql, ...){

	va_list args;
	va_start(args, sql);

	int retval = execure_query_va(cursor, sql, args);

	va_end(args);

	return retval;
}

int SqliteWrapper::execute_update(const char *sql, ...) {

	int rc, i, z;
	va_list args;
	va_start(args, sql);

	string newSql = translateSql(sql);


	sqlite3_stmt *pStmt = NULL;
	rc = sqlite3_prepare_v2(pDB, newSql.c_str(), -1, &pStmt, 0);
	if(rc != SQLITE_OK) {
		return rc;
	}

	rc = bindSql(sql, pStmt, args);
	if(rc != SQLITE_OK) {goto done;}

	rc = sqlite3_step(pStmt);

	done:
	if (pStmt) { sqlite3_finalize(pStmt); }

	va_end(args);

	return rc;
}

sqlite3_int64 SqliteWrapper::getLastInsertRow() {
	return sqlite3_last_insert_rowid(pDB);
}
