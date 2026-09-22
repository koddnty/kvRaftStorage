# 测试程序

add_executable(rpcFrameTest rpc/rpcFrameTest.cc)        # RPC 帧冒烟测试
target_link_libraries(rpcFrameTest PRIVATE KVStorageLib)


# 单元测试（Catch2）
find_package(Catch2 REQUIRED)

add_executable(unitTests
    unitTests.cc
    rpc/rpcUnitTest.cc                                  # RPC 帧协议
)
target_link_libraries(unitTests PRIVATE Catch2::Catch2WithMain KVStorageLib)
