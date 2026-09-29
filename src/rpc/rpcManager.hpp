#pragma once
#include <sylar/basic/singleton.h>

#include "rpcClient.hpp"
#include "rpcServer.hpp"

namespace craft {

// RPC 总入口（单例）
class RPCManager : public m_sylar::Singleton<RPCManager> {
public:

    void create() {
        if (m_client == nullptr) {
            m_client = std::make_shared<RPC::RPCClient>();
        }
        if (m_server == nullptr) {
            m_server = std::make_shared<RPC::RPCServer>();
        }
    }

    // 幂等：重复调用不会重建连接（GetInstance 每次都会调它）
    m_sylar::Task<void> init(const RPC::RpcDefine& define, int selfId) {
        if (m_inited) {
            co_return;
        }
        m_inited = true;
        create();
        // 装载 conf
        co_await m_client->init(define, selfId);
        co_await m_server->init(define, selfId);

        m_server->start();
    }

    RPC::RPCServer::ptr getServer() { return m_server; }
    RPC::RPCClient::ptr getClient() { return m_client; }    // ★ RPCClient 在 craft::RPC 里

private:
    bool m_inited{false};
    RPC::RPCClient::ptr m_client;
    RPC::RPCServer::ptr m_server;
};

}  // namespace craft
