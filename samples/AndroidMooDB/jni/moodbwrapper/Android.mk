LOCAL_PATH := $(call my-dir)

include $(CLEAR_VARS)

LOCAL_CXXFLAGS += -fexceptions -frtti
LOCAL_C_INCLUDES += $(LOCAL_PATH)/../libs/$(TARGET_ARCH_ABI)/include
LOCAL_MODULE    := moodbwrapper
LOCAL_SRC_FILES := MooDBWrapper.cpp MooDBCursorWrapper.cpp
#LOCAL_STATIC_LIBRARIES := moodbcore libmozjs185 libsqlite3 libpocofoundation
LOCAL_WHOLE_STATIC_LIBRARIES := moodbcore libmozjs185 libsqlite3 libpocofoundation
LOCAL_LDLIBS := -llog

include $(BUILD_SHARED_LIBRARY)