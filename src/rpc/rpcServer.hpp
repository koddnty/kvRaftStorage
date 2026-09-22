#pragma once
#include <memory>
/**
 * 实现rpc服务端
 *
 */
namespace craft {

class RPCServer : public std::enable_shared_from_this<RPCServer>, m_sylar::TcpServer{
public:
    using ptr = std::shared_ptr<RPCServer>;

};
}