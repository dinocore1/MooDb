
#ifndef COMMON_H_
#define COMMON_H_


//std c
#include <string.h>


#include <jsapi.h>
#include <uuid/uuid.h>

#include "moodb.h"
#include "sqlitewrapper.h"
#include "utils.h"
#include "js.h"

#define MAX_ERRORMSG 1024

typedef struct queryobject_t {
	char emitTable[256];
	long objectId;
} queryobject_t;

struct moodb {
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
#define LOG(format, ...) fprintf(stderr, format, __VA_ARGS__)

#endif /* COMMON_H_ */
