//
// 单元测试入口（Catch2）
//
// 各模块的测试放在对应子目录下，统一编进 unitTests 目标：
//   rpc/rpcUnitTest.cc      RPC 帧协议（Frame / Parser）
//   rpc/rpcFrameTest.cc     RPC 帧冒烟测试（独立可执行，不在此目标内）
//
// 运行： ./unitTests            全部
//        ./unitTests "[rpc]"    只跑 RPC
//
