
#include <string>

#include "moodb/moodb.h"

#include "moodb/common.h"
#include <config.h>

using namespace std;

int numOpenConnections = 0;

void* moodb_malloc(size_t size) {
	return malloc(size);
}
void moodb_free(void * ptr) {
	free(ptr);
}

void setErrorMsg(moodb *db, const char* format, ...) {
	va_list args;
	va_start(args, format);
	vsnprintf(db->lastError, MAX_ERRORMSG, format, args);
	fprintf(stderr, "moodb: %s", db->lastError);
	va_end(args);
}

int reduce(moodb *pDb, const char *viewname) {

	int retval = MOODB_ERROR;
	SqliteTransactionScope tr;
	pDb->db.beginTransaction(&tr);

	jsval viewval;
	SqliteCursor cursor;
	if(pDb->db.execute_query(&cursor, "SELECT viewSpec, dirty FROM views WHERE name = @s", viewname) != SQLITE_OK) {
		return MOODB_ERROR;
	}
	if(cursor.step() == SQLITE_ROW){
		if(cursor.getInt(1) == 0){
			//not dirty
			return MOODB_OK;
		}

		const char *viewspec = cursor.getText(0);
		if(!JS_EvaluateScript(pDb->cx, JS_GetGlobalObject(pDb->cx), viewspec, strlen(viewspec),
				__FILE__, 0, &viewval)){
			setErrorMsg(pDb, "error compiling viewspec");
			return MOODB_ERROR;
		}
	}


	jsval reducefunc;
	retval = ensureObjHasFunctionProperty(pDb, viewval, "reduce", &reducefunc);
	if(retval != MOODB_OK){
		return retval;
	}

	{
		char sqlCmd[256];
		sprintf(sqlCmd, "SELECT DISTINCT key FROM m_%s", viewname);
		retval = pDb->db.execute_query(&cursor, sqlCmd);
		if(retval != SQLITE_OK){
			return MOODB_ERROR;
		}
	}

	sprintf(pDb->currentQuery.emitTable, "r_%s", viewname);
	pDb->currentQuery.objectId = -1;

	while(cursor.step() == SQLITE_ROW){

		jsval maparg[2];

		const char *key = cursor.getText(0);
		maparg[0] = STRING_TO_JSVAL(JS_NewStringCopyZ(pDb->cx, key));

		char sqlCmd[256];
		sprintf(sqlCmd, "SELECT value FROM m_%s WHERE key = @s", viewname);

		createJSIterator(pDb, &maparg[1], sqlCmd, key);

		jsval reducerval;
		if(!JS_CallFunctionName(pDb->cx, JSVAL_TO_OBJECT(viewval), "reduce", 2, maparg, &reducerval)){
			setErrorMsg(pDb, "error running reduce function");
			return retval;
		}
	}

	retval = pDb->db.execute_update("UPDATE views SET dirty = 0 WHERE name = @s", viewname);

	tr.setSuccesfull();
	return MOODB_OK;
}

int moodb_open(const char *filename, moodb **ppDb) {
	LOG("opening database: %s", filename);

	moodb *db = (moodb*)malloc(sizeof(moodb));
	memset(db, 0, sizeof(moodb));

	//init SQL

	if(db->db.open(filename) != SQLITE_OK){
		return MOODB_ERROR;
	}

	if(db->db.execute_sql("CREATE TABLE IF NOT EXISTS objects ( _id INTEGER PRIMARY KEY, key TEXT, data TEXT, UNIQUE(key) );"
			"CREATE TABLE IF NOT EXISTS views ( name TEXT PRIMARY KEY, viewSpec TEXT, dirty INTEGER);")
			!= SQLITE_OK){
		return MOODB_ERROR;
	}

	//init JS
	if(initJS(db)){
		return MOODB_ERROR;
	}

	*ppDb = db;

	numOpenConnections++;

	return MOODB_OK;
}

void moodb_close(moodb *pDb) {
	JS_DestroyContext(pDb->cx);

	numOpenConnections--;
	if(numOpenConnections <=0){
		shutdownJS();
	}
}

int moodb_putobject(moodb *pDB, const char *id, const char* jsonData, char** ppId) {
	int retval = MOODB_ERROR;

	jsval jsdata;
	if(!JS_EvaluateScript(pDB->cx, JS_GetGlobalObject(pDB->cx), jsonData, strlen(jsonData),
			__FILE__, 0, &jsdata)){
		setErrorMsg(pDB, "error compiling data: %s", jsonData);
		return MOODB_ERROR;
	}

	uuidstr_t uuid;
	if(id == NULL) {
		//generate a new UUID for the id of this new item
		generateUUID(uuid);
		id = uuid;
	}

	SqliteTransactionScope tr;
	pDB->db.beginTransaction(&tr);

	retval = pDB->db.execute_update("INSERT OR REPLACE INTO objects (key, data) VALUES (@s, @s)", id, jsonData);
	if(retval != SQLITE_DONE) {return MOODB_ERROR;}
	pDB->currentQuery.objectId = pDB->db.getLastInsertRow();

	//update all the views
	SqliteCursor cursor;
	retval = pDB->db.execute_query(&cursor, "SELECT name, viewSpec FROM views");
	if(retval == SQLITE_OK){
		while(cursor.step() == SQLITE_ROW){
			const char *cname = cursor.getText(0);
			sprintf(pDB->currentQuery.emitTable, "m_%s", cname);

			const char *cviewspec = cursor.getText(1);

			jsval viewobj;
			if(!JS_EvaluateScript(pDB->cx, JS_GetGlobalObject(pDB->cx), cviewspec, strlen(cviewspec),
					__FILE__, 0, &viewobj)){
				setErrorMsg(pDB, "error compiling viewspec: %s", cviewspec);
				return MOODB_ERROR;
			}

			jsval maparg[] = {jsdata};
			jsval maprval;
			if(!JS_CallFunctionName(pDB->cx, JSVAL_TO_OBJECT(viewobj), "map", 1, maparg, &maprval)){
				setErrorMsg(pDB, "error running map function");
				return MOODB_ERROR;
			}

		}
	}

	tr.setSuccesfull();

	if(ppId != NULL){
		*ppId = (char*)moodb_malloc(sizeof(char) * strlen(id));
		strcpy(*ppId, id);
	}

	return MOODB_OK;
}

int moodb_getobject(moodb *pDB, const char *id, char** pJsonData) {
	int retval;
	SqliteCursor cursor;
	retval = pDB->db.execute_query(&cursor, "SELECT data FROM objects WHERE key = @s", id);
	if(retval != SQLITE_OK) {
		return MOODB_ERROR;
	}

	if(pJsonData != NULL){
		if(cursor.step() == SQLITE_ROW) {
			const char *cdata = cursor.getText(0);
			*pJsonData = (char*)moodb_malloc(sizeof(char) * (strlen(cdata) + 1));
			strcpy(*pJsonData, cdata);
		}
	}

	return MOODB_OK;
}

int moodb_deleteobject(moodb *pDB, const char *id) {
	int retval;
	retval = pDB->db.execute_update("DELETE FROM objects WHERE key = @s", id);
	return retval == SQLITE_OK ? MOODB_OK : MOODB_ERROR;
}

int moodb_putview(moodb *pDB, const char *viewspec) {
	int retval = MOODB_ERROR;
	jsval viewval;
	JSObject *viewObj;

	if(!JS_EvaluateScript(pDB->cx, JS_GetGlobalObject(pDB->cx), viewspec, strlen(viewspec),
			__FILE__, 0, &viewval)){
		setErrorMsg(pDB, "error compiling viewspec");
		return MOODB_ERROR;
	}

	if(!JSVAL_IS_OBJECT(viewval)){
		setErrorMsg(pDB, "viewspec is not an object");
		return MOODB_ERROR;
	}
	viewObj = JSVAL_TO_OBJECT(viewval);

	jsval nameVal;
	if(!JS_LookupProperty(pDB->cx, viewObj, "name", &nameVal)){
		setErrorMsg(pDB, "viewspec does not have a name property");
		retval = MOODB_ERROR;
		return MOODB_ERROR;
	}

	if(!JSVAL_IS_STRING(nameVal)){
		setErrorMsg(pDB, "name is not a string");
		return MOODB_ERROR;
	}

	jsval mapfunc;
	retval = ensureObjHasFunctionProperty(pDB, viewval, "map", &mapfunc);
	if(retval != MOODB_OK) {return MOODB_ERROR;}

	jsval reducefunc;
	JSBool hasReduce;
	if(JS_HasProperty(pDB->cx, viewObj, "reduce", &hasReduce) && hasReduce){
		retval = ensureObjHasFunctionProperty(pDB, viewval, "reduce", &reducefunc);
		if(retval != MOODB_OK) {return MOODB_ERROR;}
	}

	jsval srcval;
	if(!JS_CallFunctionName(pDB->cx, viewObj, "toSource", 0, 0, &srcval)){
		setErrorMsg(pDB, "viewspec cannot convert to source");
		return MOODB_ERROR;
	}

	JSAutoByteString data(pDB->cx, JSVAL_TO_STRING(srcval));
	JSAutoByteString name(pDB->cx, JSVAL_TO_STRING(nameVal));
	char *cdata = data.ptr();
	char *cname = name.ptr();

	SqliteTransactionScope tr;
	pDB->db.beginTransaction(&tr);

	{
		//setup the table
		retval = pDB->db.execute_update("INSERT OR REPLACE INTO views (name, viewSpec, dirty) VALUES (@s, @s, 0);", cname, cdata );
		if(retval != SQLITE_DONE) {return MOODB_ERROR;}

		char sqlcmd[512];
		sprintf(sqlcmd, "DROP TABLE IF EXISTS m_%s;", cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		sprintf(sqlcmd, "CREATE TABLE m_%s (objid INTERGER, key TEXT, value TEXT);", cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		sprintf(sqlcmd, "CREATE INDEX IF NOT EXISTS m1_%s ON m_%s(key, value);", cname, cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		sprintf(sqlcmd, "CREATE INDEX IF NOT EXISTS m2_%s ON m_%s(objid);", cname, cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		sprintf(sqlcmd, "CREATE TRIGGER IF NOT EXISTS delete_m_%s BEFORE DELETE ON objects FOR EACH ROW "
				"BEGIN "
				"DELETE FROM m_%s WHERE objid = old._id; "
				"UPDATE views SET dirty = 1 WHERE name = '%s'; "
				"END;", cname, cname, cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		sprintf(sqlcmd, "DROP TABLE IF EXISTS r_%s;", cname);
		if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

		if(hasReduce){
			sprintf(sqlcmd, "CREATE TABLE r_%s (objid INTERGER, key TEXT, value TEXT);", cname);
			if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

			sprintf(sqlcmd, "CREATE INDEX IF NOT EXISTS r1_%s ON r_%s(key, value);", cname, cname);
			if((retval = pDB->db.execute_update(sqlcmd)) != SQLITE_DONE) {return MOODB_ERROR;}

			if((retval = pDB->db.execute_update("UPDATE views SET dirty = 1 WHERE name = @s", cname)) != SQLITE_DONE){
				return MOODB_ERROR;
			}
		}

	}

	{
		//perform map function on existing objects
		sprintf(pDB->currentQuery.emitTable, "m_%s", cname);

		SqliteCursor cursor;
		if(pDB->db.execute_query(&cursor, "SELECT _id, data FROM objects") != SQLITE_OK){
			return MOODB_ERROR;
		}

		while(cursor.step() == SQLITE_ROW){

			pDB->currentQuery.objectId = cursor.getLong(0);
			const char *data = cursor.getText(1);
			jsval jsdata;
			if(!JS_EvaluateScript(pDB->cx, JS_GetGlobalObject(pDB->cx), data, strlen(data),
					__FILE__, 0, &jsdata)){
				setErrorMsg(pDB, "error compiling data: %s", data);
				return MOODB_ERROR;
			}

			jsval maparg[] = {jsdata};
			jsval maprval;
			if(!JS_CallFunctionName(pDB->cx, viewObj, "map", 1, maparg, &maprval)){
				setErrorMsg(pDB, "error running map function");
				return MOODB_ERROR;
			}
		}


	}

	tr.setSuccesfull();

	return MOODB_OK;
}

int moodb_query(moodb *pDB, moocursor **ppCursor, const char* query){

	int retval = MOODB_ERROR;
	JSAutoByteString whereClause;
	jsval queryval;
	JSObject *queryObj;

	if(!JS_EvaluateScript(pDB->cx, JS_GetGlobalObject(pDB->cx), query, strlen(query),
			__FILE__, 0, &queryval)){
		setErrorMsg(pDB, "error compiling query");
		return MOODB_ERROR;
	}

	if(!JSVAL_IS_OBJECT(queryval)){
		setErrorMsg(pDB, "query is not an object");
		return MOODB_ERROR;
	}
	queryObj = JSVAL_TO_OBJECT(queryval);

	jsval filterval;
	if(JS_LookupProperty(pDB->cx, queryObj, "filter", &filterval)){
		if(!JSVAL_IS_STRING(filterval)){
			setErrorMsg(pDB, "filter is not a string");
			return MOODB_ERROR;
		}

		//create a new parser and parse the filter
		jsval parserval;
		if(!JS_ExecuteScript(pDB->cx, JS_GetGlobalObject(pDB->cx), queryParser, &parserval)){
			setErrorMsg(pDB, "error creating new parser");
			return MOODB_ERROR;
		}

		jsval parsearg[] = {filterval};
		jsval parserresult;
		if(!JS_CallFunctionName(pDB->cx, JSVAL_TO_OBJECT(parserval), "parse", 1, parsearg, &parserresult)){
			setErrorMsg(pDB, "error parsing filter");
			return MOODB_ERROR;
		}

		whereClause.encode(pDB->cx, JSVAL_TO_STRING(parserresult));
	}


	jsval viewVal;
	if(JS_LookupProperty(pDB->cx, queryObj, "view", &viewVal)){
		if(!JSVAL_IS_STRING(viewVal)){
			setErrorMsg(pDB, "view is not a string");
			return MOODB_ERROR;
		}

		char sqlCmd[256];
		{
			JSAutoByteString viewname(pDB->cx, JSVAL_TO_STRING(viewVal));
			char *cviewname = viewname.ptr();
			if(cviewname[0] == 'r' && cviewname[1] == '_'){
				//perform reduce
				retval = reduce(pDB, &cviewname[2]);
				if(retval != MOODB_OK){return MOODB_ERROR;}
			}
			sprintf(sqlCmd, "SELECT key, value FROM %s", cviewname);
		}

		if(whereClause.ptr()){
			sprintf(sqlCmd, "%s WHERE %s;", sqlCmd, whereClause.ptr());
		}

		moocursor* cursor = new moocursor;
		cursor->pDB = pDB;

		retval = pDB->db.execute_query(&cursor->cursor, sqlCmd);
		if(retval == SQLITE_OK){
			*ppCursor = cursor;
		}

	}

	return MOODB_OK;
}

int moodbcursor_next(moocursor *pCursor, char **ppKey, char **ppValue) {
	int retval = MOODB_ERROR;
	int rc;
	rc = pCursor->cursor.step();
	if(rc == SQLITE_ROW){
		if(*ppKey != NULL){
			*ppKey = (char*)pCursor->cursor.getText(0);
		}
		if(*ppKey != NULL){
			*ppValue = (char*)pCursor->cursor.getText(1);
		}
		retval = MOODB_OK;
	}

	return retval;
}

int moodbcursor_close(moocursor *pCursor){
	delete pCursor;
	return MOODB_OK;
}

/*
i = {
    map: function(obj) {
        this.emit("hi", 1);
        this.emit("hi", 1);
        this.emit("cool", "rad");
        },
    emit: function(key, value) {
        var row = this.values[key];
        if(row == undefined){
            row = new Array();
            this.values[key] = row;
        }
        row[row.length] = value;
        },
    values: {}
    };


i.map();
i
 */
