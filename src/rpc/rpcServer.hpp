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
    using HandlerFunc = std::function<m_sylar::Task<void>(std::shared_ptr<Request> req, RPCSession::ptr)>;
    bool start() override;

    void registeRoute(const std::string& service, const std::string& method, HandlerFunc func);


protected:
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> startAccept(m_sylar::Socket::ptr sock) override;
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> handleClient(m_sylar::Socket::ptr client) override;
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> coRoute(std::shared_ptr<Request> req, RPCSession::ptr session);

private:
    std::mutex m_mutex;

    std::map<std::string, std::map<std::string, HandlerFunc>> m_route;
    // repair: 删掉了这里的 `std::vector<m_sylar::Socket::ptr> m_sockets;` 和
    //   `m_sylar::IOManager* m_iomanager {nullptr};` —— TcpServer 里这两个都是 **private**，
    //   基类只给了受保护的 getSockets() / getIomanager() 两个访问口。在派生类里再声明一遍
    //   不是"覆盖"，是【影子】：变成完全独立的另一个变量。
    //   后果是致命的：TcpServer::bind() 往【基类】的 m_sockets 里塞监听 socket，
    //   而 RPCServer::start() 遍历的是【派生类】那个空的 m_sockets → 循环一次都不执行
    //   → startAccept 永远不被调度 → 连接躺在内核 accept 队列里没人接
    //   → 客户端 send 成功、却永远等不到回包（实测就是这个现象）。
    //   统一改用 getSockets() / getIomanager()。
    int m_node_id{-1};
};


}
}