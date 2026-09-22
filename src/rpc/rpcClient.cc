#include "rpcClient.hpp"
#include <sylar/basic/log.h>
namespace craft {
namespace RPC {
static auto g_logger = M_SYLAR_LOG_NAME("craft");


m_sylar::Task<int> RPCClient::init() {

}

m_sylar::Task<RPCClient::State> RPCClient::call(    int node_id,
                                                    const std::string& service,
                                                    const std::string& method,
                                                    const std::string& req_bytes,
                                                    std::string* resp_bytes) {
    // node状态确认
    if (node_id >= m_peers.size() || m_peers[node_id].state != NodeState::READY) {
        M_SYLAR_LOG_WARN(g_logger) << "Failed to RPC call with invalid node or unconnected socket, node id = "
                                     << node_id << " service : " << service << " method : " << method;
        co_return State::FAILED;
    }

    auto node = m_peers[node_id];
    m_sylar::Socket::ptr socket = node.sock;

    socket->send(req_bytes);
}

m_sylar::Task<int> RPCClient::reset() {

}


}
}