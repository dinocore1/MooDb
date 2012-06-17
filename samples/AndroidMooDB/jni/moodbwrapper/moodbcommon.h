#include "MooDBWrapper.h"
#include "MooDBCursorWrapper.h"
#include "moodb/moodb.h"

#include "Poco/Logger.h"
#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include "Poco/Message.h"

#include <android/log.h>
#define LOG_TAG "moodbwraper"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO,LOG_TAG,__VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR,LOG_TAG,__VA_ARGS__)

using Poco::Logger;
using Poco::Channel;
using Poco::Message;
using Poco::FastMutex;
