
#include "moodbcommon.h"


class AndroidLogChannel : public Channel {
public:
	AndroidLogChannel(){};

	void log(const Message& msg) {
		FastMutex::ScopedLock lock(_mutex);
		LOGI("%s", msg.getText().c_str());
	}

protected:
	~AndroidLogChannel(){};

private:
	static FastMutex _mutex;
};

FastMutex AndroidLogChannel::_mutex;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
	JNIEnv* env;
	if (vm->GetEnv((void**)&env, JNI_VERSION_1_6) != JNI_OK)
		return -1;

	/* get class with (*env)->FindClass */
	/* register methods with (*env)->RegisterNatives */


	Logger::root().setChannel(new AndroidLogChannel());


	return JNI_VERSION_1_6;
}

jfieldID sNativePointerField = 0;
moodb* getNativeData(JNIEnv *env, jobject thiz) {
	if(sNativePointerField == 0){
		jclass clazz = env->GetObjectClass(thiz);
		sNativePointerField = env->GetFieldID(clazz, "mNativepointer", "J");
	}
	return (moodb*)env->GetLongField(thiz, sNativePointerField);
}

JNIEXPORT jint JNICALL Java_com_devsmart_moodb_MooDBWrapper_moodbopen
(JNIEnv *env, jobject thiz, jstring js_filepath) {

	const char *filepath = env->GetStringUTFChars(js_filepath, 0);

	moodb* pDB;
	if(moodb_open(filepath, &pDB) == MOODB_OK){
		LOGI("Success opening db: %s", filepath);
		jclass clazz = env->GetObjectClass(thiz);
		jfieldID nativePointerField = env->GetFieldID(clazz, "mNativepointer", "J");
		env->SetLongField(thiz, nativePointerField, (jlong)pDB);
	} else {
		LOGE("Error opening db: %s", filepath);
	}

	env->ReleaseStringUTFChars(js_filepath, filepath);


}

JNIEXPORT jstring JNICALL Java_com_devsmart_moodb_MooDBWrapper_putObject
(JNIEnv *env, jobject thiz, jstring js_key, jstring js_data) {

	jstring retval;

	moodb* pDB = getNativeData(env, thiz);

	const char *key = NULL;
	if(js_key != NULL){
		key = env->GetStringUTFChars(js_key, 0);
	}
	const char *data = env->GetStringUTFChars(js_data, 0);

	char *pOutKey;
	if(moodb_putobject(pDB, key, data, &pOutKey) == MOODB_OK){
		LOGI("put key: %s", pOutKey);
		retval = env->NewStringUTF(pOutKey);
		moodb_free(pOutKey);
	} else {
		LOGE("could not put object: %s", key);
	}

	if(js_key != NULL){
		env->ReleaseStringUTFChars(js_key, key);
	}
	env->ReleaseStringUTFChars(js_data, data);

	return retval;
}

JNIEXPORT void JNICALL Java_com_devsmart_moodb_MooDBWrapper_putView
(JNIEnv *env, jobject thiz, jstring js_viewspec) {

	moodb* pDB = getNativeData(env, thiz);

	const char *viewspec = env->GetStringUTFChars(js_viewspec, 0);
	if(moodb_putview(pDB, viewspec) == MOODB_OK){
		LOGI("update view success");
	} else {
		char *errorstr;
		moodb_getlasterror(pDB, &errorstr);
		LOGE("error updating view: %s", errorstr);
	}

	env->ReleaseStringUTFChars(js_viewspec, viewspec);

}

JNIEXPORT jobject JNICALL Java_com_devsmart_moodb_MooDBWrapper_query
(JNIEnv *env, jobject thiz, jstring js_query) {


	jclass cursorclass = env->FindClass("com/devsmart/moodb/MooDBCursor");
	if(cursorclass == NULL){
		LOGE("could not find MooDBCursor class");
		return NULL;
	}

	jmethodID cid = env->GetMethodID(cursorclass, "<init>", "()V");
	if(cid == NULL){
		LOGE("could not find MooDBCursor constructor");
		return NULL;
	}



	moodb* pDB = getNativeData(env, thiz);
	const char *query = env->GetStringUTFChars(js_query, 0);

	moocursor *nativeCursor;
	if(moodb_query(pDB, &nativeCursor, query) != MOODB_OK){
		LOGE("error in query");
		env->ReleaseStringUTFChars(js_query, query);
		return NULL;
	}
	env->ReleaseStringUTFChars(js_query, query);

	jobject retval = env->NewObject(cursorclass, cid);

	//set the native pointer
	jfieldID npfield = env->GetFieldID(cursorclass, "mNativepointer", "J");
	env->SetLongField(retval, npfield, (jlong)nativeCursor);


	return retval;
}

