#pragma once

#include <memory>
#include <server/common/session.hpp>
#include <sylar/coroutine/corobase.h>
#include <sylar/basic/socket.h>
#include <sylar/basic/lock.hpp>
#include "payload.pb.h"
#include "frame.hpp"
#include "parser.hpp"


namespace craft {
namespace RPC {

enum class SessionState {
    INITING,        // 没初始化
    READY,          // 可用
    BUSY,           // 被占用，繁忙
    OFFLINE,        // 离线（超时，退出等状态）
    ERROR           // 发生错误
};      // 节点状态


class RPCSession : public m_sylar::Session {
    public:
        using ptr = std::shared_ptr<RPCSession>;
        RPCSession(m_sylar::Socket::ptr sock);
        ~RPCSession();

        // 对当前节点调用方法
        m_sylar::Task<m_sylar::IOState> call(std::shared_ptr<std::string> service,   // 服务
                                      std::shared_ptr<std::string> method,    // 方法
                                      std::shared_ptr<std::string> req_bytes,
                                      std::shared_ptr<std::string> resp_bytes);

        // 返回值仍是 m_sylar::IOState，需要注意的是：返回CLOSED代表此连接发生异常（包括但不限于发送/接收失败，帧解析失败），需要重新建立连接
        m_sylar::Task<m_sylar::IOState> co_sendRequest(const Frame& frame);
        m_sylar::Task<m_sylar::IOState> co_recvResponse(Response& frame, int request_id);

        // repair: 原来 node_id 恒为 0 且没有任何赋值入口，日志里永远是 "Peer 0"，排查时会误导
        void setNodeId(int id) { node_id = id; }
        [[nodiscard]] int getNodeId() const { return node_id; }

        void setAddress(const m_sylar::IPv4Address& address) const;

    private:
        m_sylar::Task<m_sylar::IOState> recvResponseFromBuffer(Response& frame, int request_id);      // 不涉及网络，从各种缓冲中获得数据尝试解析

    private:
        SessionState m_state {SessionState::INITING};
        int node_id {-1};            // 当前对端节点node_id，配置项中应从0开始
        int m_request_id {0};        // 请求id,用于分离请求与响应

        std::shared_ptr<m_sylar::IPv4Address> m_address = std::make_shared<m_sylar::IPv4Address>();     // 对端地址

        std::map<int, std::shared_ptr<Response>> m_recved_frames;      // 记录所有接收的帧内容
        Parser::ptr m_parser{std::make_shared<Parser>()};
        m_sylar::CoMutex m_send_mutex;          // 锁写
        m_sylar::CoMutex m_recv_mutex;          // 锁读
        m_sylar::CoMutex m_buffer_mutex;        // 锁接收缓冲区
    };

}
}
