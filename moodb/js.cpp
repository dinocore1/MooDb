/*
 * js.c
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#include "common.h"
#include "jsfunctions.h"

JSRuntime *runtime = NULL;
JSObject *queryParser = NULL;

extern const unsigned char queryexpression[];


/* The class of the global object. */
static JSClass global_class = {
		"global", JSCLASS_GLOBAL_FLAGS | JSCLASS_HAS_PRIVATE,
		JS_PropertyStub, JS_PropertyStub, JS_PropertyStub, JS_StrictPropertyStub,
		JS_EnumerateStub, JS_ResolveStub, JS_ConvertStub, JS_FinalizeStub,
		JSCLASS_NO_OPTIONAL_MEMBERS
};



void reportError(JSContext *cx, const char *message, JSErrorReport *report){
	fprintf(stderr, "%s:%u:%s\n",
			report->filename ? report->filename : "<no filename>",
					(unsigned int) report->lineno,
					message);
}

/* Native js functions */

JSBool js_log(JSContext *cx, uintN argc, jsval *vp) {
	JSString *msg;
	if (!JS_ConvertArguments(cx, argc, JS_ARGV(cx, vp), "S", &msg))
		return JS_FALSE;

	JSAutoByteString msgstr(cx, msg);

	printf("JS: %s", msgstr.ptr());

	JS_SET_RVAL(cx, vp, JSVAL_VOID);
	return JS_TRUE;
}

JSBool js_emit(JSContext *cx, uintN argc, jsval *vp) {
	JSString *key;
	JSString *value;
	if (!JS_ConvertArguments(cx, argc, JS_ARGV(cx, vp), "SS", &key, &value)) {
		return JS_FALSE;
	}

	JSAutoByteString keystr(cx, key);
	JSAutoByteString valuestr(cx, value);

#ifdef DEBUG
	LOG("\nemit: %s : %s", keystr.ptr(), valuestr.ptr());
#endif

	moodb* pDb = (moodb*)JS_GetContextPrivate(cx);

	std::string sqlcmd;
	sqlcmd += "INSERT INTO ";
	sqlcmd += pDb->currentQuery.emitTable;
	sqlcmd += " (objid, key, value) VALUES (@l, @s, @s);";
	if(pDb->db.execute_update(sqlcmd.c_str(), pDb->currentQuery.objectId, keystr.ptr(), valuestr.ptr()) != SQLITE_DONE){
		setErrorMsg(pDb, "Error inserting into %s", pDb->currentQuery.emitTable.c_str());
		return JS_FALSE;
	}

	return JS_TRUE;

}

JSFunctionSpec globalHelpers[] = {
		JS_FN("log", js_log, 1, 0),
		JS_FN("emit", js_emit, 2, 0),
		JS_FS_END
};

JSBool js_itrnext(JSContext *cx, uintN argc, jsval *vp) {

	SqliteCursor *pSqlStmt = (SqliteCursor*)JS_GetPrivate(cx, JS_THIS_OBJECT(cx, vp));
	if(pSqlStmt->step() == SQLITE_ROW){
		const char *cobj = pSqlStmt->getText(0);
		jsval jsdata;
		if(!JS_EvaluateScript(cx, JS_GetGlobalObject(cx), cobj, strlen(cobj),
				__FILE__, 0, &jsdata)){
			return JS_FALSE;
		}
		JS_SetReservedSlot(cx, JS_THIS_OBJECT(cx, vp), 0, jsdata);

		JS_SET_RVAL(cx, vp, JSVAL_TRUE);
		return JS_TRUE;
	}

	JS_SET_RVAL(cx, vp, JSVAL_FALSE);
	return JS_TRUE;
}

JSBool js_itrvalue(JSContext *cx, uintN argc, jsval *vp) {

	jsval rval;
	JS_GetReservedSlot(cx, JS_THIS_OBJECT(cx, vp), 0, &rval);
	JS_SET_RVAL(cx, vp, rval);
	return JS_TRUE;
}

void js_itrfinalize(JSContext *cx, JSObject *obj) {
	SqliteCursor *pSqlStmt = (SqliteCursor*)JS_GetPrivate(cx, obj);
	delete pSqlStmt;
}

JSFunctionSpec nativeIter[] = {
		JS_FN("next", js_itrnext, 0, 0),
		JS_FN("value", js_itrvalue, 0, 0),
		JS_FS_END
};

static JSClass sqlcursor_class = {
		"sqlcursor", JSCLASS_HAS_PRIVATE | JSCLASS_HAS_RESERVED_SLOTS(1),
		JS_PropertyStub, JS_PropertyStub, JS_PropertyStub, JS_StrictPropertyStub,
		JS_EnumerateStub, JS_ResolveStub, JS_ConvertStub, js_itrfinalize,
		JSCLASS_NO_OPTIONAL_MEMBERS
};


int initJS(moodb *pDb) {
	if(!runtime){
		runtime = JS_NewRuntime(8*1024*1024);
	}

	JSObject  *global;
	//start new javascript context
	JSContext *cx = JS_NewContext(runtime, 8192);

	JS_SetOptions(cx, JSOPTION_STRICT | JSOPTION_VAROBJFIX);
	JS_SetErrorReporter(cx, reportError);

#ifdef DEBUG
	JS_SetGCZeal(cx, 2);
#endif

	/* Create the global object in a new compartment. */
	global = JS_NewCompartmentAndGlobalObject(cx, &global_class, NULL);
	if (global == NULL)
		return MOODB_ERROR;

	JS_InitStandardClasses(cx, global);

	JS_DefineFunctions(cx, global, globalHelpers);

	JS_SetContextPrivate(cx, pDb);

	{
		const char *srcStr = JSPREPAREFUNC;
		const char* argnames[] = {"queryObj"};
		if(!JS_CompileFunction(cx, global, "prepareQuery", 1, argnames, srcStr, strlen(srcStr), __FUNCTION__, __LINE__)){
			setErrorMsg(pDb, "error compiling prepareQuery");
			return MOODB_ERROR;
		}
	}


	if(queryParser == NULL){
		const char* querystr = reinterpret_cast<const char*>(queryexpression);
		size_t length = strlen(querystr);
		queryParser = JS_CompileScript(cx, NULL, querystr, length, __FUNCTION__, __LINE__);
		if(queryParser == NULL){
			setErrorMsg(pDb, "error compiling queryexpression");
			return MOODB_ERROR;
		}

		if(!JS_AddNamedObjectRoot(cx, &queryParser, "queryParser")){
			setErrorMsg(pDb, "unable to root queryParser");
			return MOODB_ERROR;
		}
	}


	pDb->cx = cx;

	return MOODB_OK;
}

int shutdownJS(){
	JS_DestroyRuntime(runtime);
	JS_ShutDown();
}

int ensureObjHasFunctionProperty(moodb *pDB, jsval obj, const char *functionName, jsval *rval){
	int retval = MOODB_ERROR;

	if(!JS_LookupProperty(pDB->cx, JSVAL_TO_OBJECT(obj), functionName, rval)){
		setErrorMsg(pDB, "object does not have a %s property", functionName);
		return MOODB_ERROR;
	}

	if(!JSVAL_IS_OBJECT(*rval)){
		setErrorMsg(pDB, "%s property is not a function", functionName);
		return MOODB_ERROR;
	}

	JSObject *functionObj = JSVAL_TO_OBJECT(*rval);
	if(!JS_ObjectIsFunction(pDB->cx, functionObj)){
		setErrorMsg(pDB, "map property is not a function");
		return MOODB_ERROR;
	}

	return MOODB_OK;
}


#ifdef DEBUG

void printJSObject(JSContext *cx, jsval val) {
	jsval rval;
	if(!JS_CallFunctionName(cx, JSVAL_TO_OBJECT(val), "toSource", 0, 0, &rval)){
		printf("convert to source");
		return;
	}
	JSAutoByteString data(cx, JSVAL_TO_STRING(rval));
	printf("\njsval: %s\n", data.ptr());
}

#endif

void createJSIterator(moodb *pDb, jsval *prval, const char *sqlCmd, ...) {

	va_list args;
	va_start(args, sqlCmd);

	SqliteCursor *cursor = new SqliteCursor();
	pDb->db.execure_query_va(cursor, sqlCmd, args);

	JSObject *retval = JS_NewObject(pDb->cx, &sqlcursor_class, NULL, NULL);
	*prval = OBJECT_TO_JSVAL(retval);
	JS_DefineFunctions(pDb->cx, retval, nativeIter);
	JS_SetPrivate(pDb->cx, retval, cursor);

}


