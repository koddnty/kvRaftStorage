# 测试程序

add_executable(rpcFrameTest rpc/rpcFrameTest.cc)        # RPC 帧冒烟测试
target_link_libraries(rpcFrameTest PRIVATE KVStorageLib)

# RPC 端到端：真实 TCP + 假服务端，覆盖往返/错误码/大响应/超时/迟到响应
add_executable(rpcSessionTest rpc/rpcSessionTest.cc)
target_link_libraries(rpcSessionTest PRIVATE KVStorageLib)

# RPCClient 重连语义：假服务端可上下线，覆盖"掉线后 call 快速失败不卡死"、
# "coConnect 有限次尝试后放弃"、"对端恢复后重连成功"（带 alarm 看门狗防卡死）
add_executable(rpcClientTest rpc/rpcClientTest.cc)
target_link_libraries(rpcClientTest PRIVATE KVStorageLib)

# rpc 配置反序列化：FormatConversion 的各种畸形输入 + 走真实 ConfigManager 读 conf/rpc.json
# 并真的建一个 RPCClient（KVRAFT_SRC_DIR 给测试定位仓库里的 conf/）
add_executable(rpcConfigTest rpc/rpcConfigTest.cc)
target_link_libraries(rpcConfigTest PRIVATE KVStorageLib)
target_compile_definitions(rpcConfigTest PRIVATE KVRAFT_SRC_DIR="${CMAKE_SOURCE_DIR}")

# 注意：rpc/peerConcurrencyTest.cc 是上一版（Peer 类）的测试，用的是已废弃的 API，
#       没有登记在这里，所以不会被构建。要么适配到 RPCSession，要么删掉。

# 单元测试（Catch2）
find_package(Catch2 REQUIRED)

add_executable(unitTests
    unitTests.cc
    rpc/rpcUnitTest.cc                                  # RPC 帧协议
)
target_link_libraries(unitTests PRIVATE Catch2::Catch2WithMain KVStorageLib)
