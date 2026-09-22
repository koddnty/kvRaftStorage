#pragma once
#include <memory>

#include <sylar/server/tcp/tcpServer.h>
/**
 * 实现rpc服务端
 *
 */
namespace craft {

class RPCServer : public m_sylar::TcpServer{
public:
    using ptr = std::shared_ptr<RPCServer>;

};
}