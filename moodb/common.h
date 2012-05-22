
#ifndef COMMON_H_
#define COMMON_H_


#include <string.h>
#include <string>


#include <jsapi.h>

#include "moodb.h"
#include "sqlitewrapper.h"
#include "utils.h"
#include "js.h"

#define MAX_ERRORMSG 1024

typedef struct queryobject_t {
	std::string emitTable;
	long objectId;
} queryobject_t;

struct moodb {
public:
	moodb();
	Poco::Logger& logger;
	JSContext *cx;
	SqliteWrapper db;
	queryobject_t currentQuery;
	char lastError[MAX_ERRORMSG];
};

struct moocursor {
	moodb *pDB;
	JSObject *script;
	SqliteCursor cursor;
};



#ifndef NULL
#define NULL 0
#endif

void setErrorMsg(moodb *db, const char* format, ...);

#endif /* COMMON_H_ */
