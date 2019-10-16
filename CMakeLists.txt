cmake_minimum_required(VERSION 3.15)
project(IDFS)

set(CMAKE_CXX_STANDARD 14)

find_package(Threads)

set(SOURCES
        storage/libs/httplib.h
        storage/masterNode.cpp
        storage/dataNode.cpp
        storage/masterNode.h
        storage/dataNode.h
        )

add_executable(IDFS_MASTER
                storage/mainMaster.cpp
                ${SOURCES})

add_executable(IDFS_DATA_NODE
                storage/mainDataNode.cpp
                ${SOURCES})

target_link_libraries(IDFS_MASTER ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(IDFS_DATA_NODE ${CMAKE_THREAD_LIBS_INIT})