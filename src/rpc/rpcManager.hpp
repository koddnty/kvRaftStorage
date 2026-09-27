#pragma once
#include <sylar/basic/singleton.h>

#include "rpcClient.hpp"
#include "rpcServer.hpp"

namespace craft {

// RPC 总入口（单例）
//
//    static T* GetInstance() { static T c; c.init(); return &c; }
//    所以本类【必须】有 init()，而且每次 GetInstance 都会调用它 → 必须幂等。
class RPCManager : public m_sylar::Singleton<RPCManager> {
public:
    // 幂等：重复调用不会重建连接（GetInstance 每次都会调它）
    void init() {
        if (m_inited) {
            return;
        }
        m_inited = true;
        // TODO: 装载 conf → 建 Peer 连接 / 起 RPCServer
        
    }

    RPCServer::ptr getServer() { return m_server; }
    RPC::RPCClient::ptr getClient() { return m_client; }    // ★ RPCClient 在 craft::RPC 里

private:
    bool m_inited{false};
    RPC::RPCClient::ptr m_client;
    RPCServer::ptr m_server;
};

}  // namespace craft
