# 测试程序

add_executable(rpcFrameTest rpc/rpcFrameTest.cc)        # RPC 帧冒烟测试
target_link_libraries(rpcFrameTest PRIVATE KVStorageLib)

# RPC 端到端：真 RPCServer + 真 TCP loopback，覆盖往返/未注册 service+method 的错误码/
# 大响应/不回包超时/迟到响应/并发在途
add_executable(rpcSessionTest rpc/rpcSessionTest.cc)
target_link_libraries(rpcSessionTest PRIVATE KVStorageLib)

# RPCClient 重连语义：peer0 = 真 RPCServer，peer1 = 没人监听的死端口。覆盖
# "对端不在线时 call 快速失败不卡死"、"coConnect 有限次尝试后放弃"、"对端恢复后重连"
# （带 alarm 看门狗防卡死）
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
    tools/argParserTest.cc                              # 命令行参数解析
)
target_link_libraries(unitTests PRIVATE Catch2::Catch2WithMain KVStorageLib)
