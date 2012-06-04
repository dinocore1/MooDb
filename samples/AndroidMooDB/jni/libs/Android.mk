LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)
LOCAL_MODULE := moodbcore
LOCAL_SRC_FILES := $(TARGET_ARCH_ABI)/lib/libmoodb.a
include $(PREBUILT_STATIC_LIBRARY)

include $(CLEAR_VARS)
LOCAL_MODULE := libmozjs185
LOCAL_SRC_FILES := $(TARGET_ARCH_ABI)/lib/libmozjs185-1.0.a
include $(PREBUILT_STATIC_LIBRARY)

include $(CLEAR_VARS)
LOCAL_MODULE := libsqlite3
LOCAL_SRC_FILES := $(TARGET_ARCH_ABI)/lib/libsqlite3.a
include $(PREBUILT_STATIC_LIBRARY)

include $(CLEAR_VARS)
LOCAL_MODULE := libpocofoundation
LOCAL_SRC_FILES := $(TARGET_ARCH_ABI)/lib/libPocoFoundation.a
include $(PREBUILT_STATIC_LIBRARY)


#include $(CLEAR_VARS)
#LOCAL_MODULE := libmoodb
#LOCAL_WHOLE_STATIC_LIBRARIES := moodbcore libmozjs185 libsqlite3 libpocofoundation
#LOCAL_LDLIBS := -lz
#include $(BUILD_SHARED_LIBRARY)