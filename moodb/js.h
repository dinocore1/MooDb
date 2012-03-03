/*
 * js.h
 *
 *  Created on: Feb 18, 2012
 *      Author: paul
 */

#ifndef JS_H_
#define JS_H_

#include "common.h"

extern JSFunction *jsqueremitFunction;
extern JSObject *queryParser;

int initJS(moodb *pDb);
int shutdownJS();

void createJSIterator(moodb *pDb, jsval *prval, const char *sqlCmd, ...);
int ensureObjHasFunctionProperty(moodb *pDB, jsval obj, const char *functionName, jsval *rval);

#ifdef DEBUG
void printJSObject(JSContext *cx, jsval val);
#endif

JSBool moodb_insert(JSContext *cx, uintN argc, jsval *vp);

#endif /* JS_H_ */
