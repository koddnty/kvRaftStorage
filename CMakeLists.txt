cmake_minimum_required(VERSION 3.10)
project(KVRaftStorage)

set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -rdynamic -g -fPIC -ggdb -std=c++20 -Wall -Wno-deprecated  -Wno-unused-function -Wno-builtin-macro-redefined -Wno-deprecated-declarations")

link_directories(/usr/local/lib)

# 项目文件
file(GLOB_RECURSE ALL_SOURCES "src/*.cc")
list(REMOVE_ITEM ALL_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cc")

# 项目依赖
find_package(yaml-cpp REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json REQUIRED)
find_library(MARIADB_LIB mariadb /usr/local/lib/mariadb)
find_library(HIREDIS_LIB hiredis /usr/local/lib)
find_library(SYLAR_LIB sylar /usr/local/lib)
find_package(CURL REQUIRED)

set (LIB_LIB
    pthread
    dl
    yaml-cpp::yaml-cpp
    OpenSSL::SSL
    OpenSSL::Crypto
    ${SYLAR_LIB}
    ${MARIADB_LIB}
    ${HIREDIS_LIB}
)


# 编译 归档 KVStorage库
add_library(KVStorageLib STATIC ${ALL_SOURCES})
target_include_directories(KVStorageLib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    /usr/local/include/sylar
)
target_link_libraries(KVStorageLib PUBLIC ${LIB_LIB})


# 编译链接可执行程序kvRaft
add_executable(kvRaft src/main.cc)
target_link_libraries(kvRaft PRIVATE KVStorageLib)





add_subdirectory(${PROJECT_SOURCE_DIR}/tests)


