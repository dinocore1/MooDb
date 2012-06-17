
#include "moodbcommon.h"

jfieldID sMoodbCursorNativeField = 0;
moocursor* getMoodbCursorNativeData(JNIEnv *env, jobject thiz) {
	if(sMoodbCursorNativeField == 0){
		jclass clazz = env->GetObjectClass(thiz);
		sMoodbCursorNativeField = env->GetFieldID(clazz, "mNativepointer", "J");
	}
	return (moocursor*)env->GetLongField(thiz, sMoodbCursorNativeField);
}

jfieldID sMoodbCursorKeyField = 0;
jfieldID sMoodbCursorValueField = 0;
void setMoodbCursorKeyAndValue(JNIEnv *env, jobject thiz, char* key, char* value) {
	if(sMoodbCursorKeyField == 0){
		jclass clazz = env->GetObjectClass(thiz);
		sMoodbCursorKeyField = env->GetFieldID(clazz, "key", "Ljava/lang/String;");
	}

	if(sMoodbCursorValueField == 0){
		jclass clazz = env->GetObjectClass(thiz);
		sMoodbCursorValueField = env->GetFieldID(clazz, "value", "Ljava/lang/String;");
	}

	jstring j_key = env->NewStringUTF(key);
	env->SetObjectField(thiz, sMoodbCursorKeyField, j_key);

	jstring j_value = env->NewStringUTF(value);
	env->SetObjectField(thiz, sMoodbCursorValueField, j_value);
}

JNIEXPORT void JNICALL Java_com_devsmart_moodb_MooDBCursor_cursorclose
(JNIEnv *env, jobject thiz) {

	moocursor* ncursor = getMoodbCursorNativeData(env, thiz);
	moodbcursor_close(ncursor);
}

JNIEXPORT jboolean JNICALL Java_com_devsmart_moodb_MooDBCursor_next
(JNIEnv *env, jobject thiz) {

	jboolean retval = JNI_TRUE;
	moocursor* ncursor = getMoodbCursorNativeData(env, thiz);
	char *key, *value;

	if(moodbcursor_next(ncursor, &key, &value) == MOODB_OK ){
		retval = JNI_TRUE;
		setMoodbCursorKeyAndValue(env, thiz, key, value);
	} else {
		retval = JNI_FALSE;
	}


	return retval;
}
