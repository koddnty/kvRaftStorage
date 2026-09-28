#pragma once
#include <memory>

#include <sylar/server/tcp/tcpServer.h>
#include "rpcClient.hpp"
/**
 * 实现rpc服务端
 *
 */
namespace craft {
namespace RPC {

class RPCServer : public m_sylar::TcpServer{
public:
    using ptr = std::shared_ptr<RPCServer>;
    using HandlerFunc = m_sylar::Task<void>(*)(RPC::RPCSession::ptr);
    bool start() override;
private:
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> startAccept(m_sylar::Socket::ptr sock) override;
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> handleClient(m_sylar::Socket::ptr client) override;

private:
    std::map<std::string, std::map<std::string, HandlerFunc>> m_route;
    int m_node_id{-1};
};


}
}