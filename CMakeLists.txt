cmake_minimum_required(VERSION 2.8.4)


project(MooDBCore)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

file(GLOB SRC_CORE src/core/*.cpp)

set(SOURCE_FILES ${SRC_CORE})

find_library(db_cxx REQUIRED)
include_directories(${db_cxx_INCLUDE_DIRS})
set(LIBS ${LIBS} ${db_cxx_LIBRARIES})

include_directories(include)
add_library(MooDBCore ${SOURCE_FILES})
target_link_libraries(MooDBCore db_cxx)


add_executable(MooDBCoreTest "src/test.cpp")
target_link_libraries(MooDBCoreTest MooDBCore)