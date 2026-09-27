# 测试程序

add_executable(rpcFrameTest rpc/rpcFrameTest.cc)        # RPC 帧冒烟测试
target_link_libraries(rpcFrameTest PRIVATE KVStorageLib)

# RPC 端到端：真实 TCP + 假服务端，覆盖往返/错误码/大响应/超时/迟到响应
add_executable(rpcSessionTest rpc/rpcSessionTest.cc)
target_link_libraries(rpcSessionTest PRIVATE KVStorageLib)

# 注意：rpc/peerConcurrencyTest.cc 是上一版（Peer 类）的测试，用的是已废弃的 API，
#       没有登记在这里，所以不会被构建。要么适配到 RPCSession，要么删掉。

# 单元测试（Catch2）
find_package(Catch2 REQUIRED)

add_executable(unitTests
    unitTests.cc
    rpc/rpcUnitTest.cc                                  # RPC 帧协议
)
target_link_libraries(unitTests PRIVATE Catch2::Catch2WithMain KVStorageLib)
