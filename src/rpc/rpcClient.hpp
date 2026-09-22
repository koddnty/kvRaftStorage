#pragma once
#include <memory>
#include <sylar/server/tcp/tcpServer.h>
#include <sylar/coroutine/corobase.h>
/**
 * 实现rpc客户端
 *
 */
namespace craft {
namespace RPC {
class RPCClient : public std::enable_shared_from_this<RPCClient> {
public:
    using ptr = std::shared_ptr<RPCClient>;
    enum class State {
        UNK_NODE,
        UNK_SERVICE,
        UNK_METHOD,
        UNK_REQBYTES,
        FAILED
    };      // call响应状态

    /**
     * @brief 初始化客户端
     */
    m_sylar::Task<int> init();

    /**
     *  @brief 远程RPC调用
     */
    m_sylar::Task<State> call(int node_id,
                              const std::string& service,   // 服务
                              const std::string& method,    // 方法
                              const std::string& req_bytes,
                              std::string* resp_bytes);

    m_sylar::Task<int> reset();     // 重置客户端

private:
    enum class NodeState {
        INITING,
        READY,
        BUSY,
        OFFLINE,        // 离线
        ERROR
    };
    // 对端接口体
    struct Peer {
        std::string      ip;
        uint16_t         port{0};
        m_sylar::Socket::ptr sock {nullptr};
        NodeState        state{NodeState::INITING};
        bool             inflight{false};
    };
    std::vector<Peer> m_peers;       // 当前网络中的其他节点
};

}
}