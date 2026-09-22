#pragma once
#include <memory>
#include <sylar/server/tcp/tcpServer.h>
/**
 * 实现rpc客户端
 *
 */
namespace craft {

class RPCClient : public std::enable_shared_from_this<RPCClient> {
public:
    using ptr = std::shared_ptr<RPCClient>;
};


}