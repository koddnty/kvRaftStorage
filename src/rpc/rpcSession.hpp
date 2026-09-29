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
// tools ----------------------------------------------------------------
// repair: errno → m_sylar::IOState
static m_sylar::IOState errnoToIOState(int err) {
    switch (err) {
    case ETIMEDOUT:                 // 等数据超时
        return m_sylar::IOState::TIMEOUT;

    case EPIPE:                     // 对端已关，本端再写就 EPIPE
    case ECONNRESET:                // 对端发了 RST
    case ECONNABORTED:              // 连接被中止
    case ENOTCONN:                  // socket 未连接
    case ESHUTDOWN:                 // 已 shutdown
    case EBADF:                     // 本地 fd 无效
        return m_sylar::IOState::CLOSED;

    default:
        return m_sylar::IOState::FAILED;
    }
}

// 仅用于日志
static const char* ioStateName(m_sylar::IOState st) {
    switch (st) {
    case m_sylar::IOState::SUCCESS: return "SUCCESS";
    case m_sylar::IOState::TIMEOUT: return "TIMEOUT";
    case m_sylar::IOState::FAILED:  return "FAILED";
    case m_sylar::IOState::UNKNOWN: return "UNKNOWN";
    case m_sylar::IOState::INIT:    return "INIT";
    case m_sylar::IOState::CLOSED:  return "CLOSED";
    }
    return "?";
}


enum class SessionState {
    INITING,        // 没初始化
    READY,          // 可用
    BUSY,           // 被占用，繁忙
    OFFLINE,        // 离线（超时，退出等状态）
    ERROR           // 发生错误
};      // 节点状态
//服务端响应码，对应 Request/Response 的 code 字段。
enum class RpcCode : int {
    OK          = 0,        // 成功
    BAD_REQUEST = 1001,     // 请求体解析失败
    UNK_SERVICE = 1002,     // 服务端没有注册这个 service
    UNK_METHOD  = 1003,     // 该 service 下没有这个 method
    INTERNAL    = 1004,     // 服务端处理时内部错误
};

class RPCSession : public m_sylar::Session {
public:
    using ptr = std::shared_ptr<RPCSession>;
    RPCSession(m_sylar::Socket::ptr sock);
    ~RPCSession();

    // 对当前节点调用方法
    m_sylar::Task<m_sylar::IOState> call(std::string service,   // 服务
                                    std::string method,    // 方法
                                    std::shared_ptr<std::string> req_bytes,
                                    std::shared_ptr<std::string> resp_bytes);

    /**
     *  @brief 客户端
     * @return  返回CLOSED代表此连接发生异常（包括但不限于发送/接收失败，帧解析失败），需要重新建立连接
    */
    m_sylar::Task<m_sylar::IOState> co_sendMessage(const Frame& frame);     // 向对端发送数据
    m_sylar::Task<m_sylar::IOState> co_recvResponse(Response& frame, int request_id);       // 接收一个response
    m_sylar::Task<m_sylar::IOState> co_recvRequest(Request& req);       // 接收一个request


    void setNodeId(int id) { node_id = id; }
    [[nodiscard]] int getNodeId() const { return node_id; }

    void setAddress(const m_sylar::IPv4Address& address) const;

    SessionState getState() {return m_state; }
    void setState(const SessionState state) {m_state = state; }

private:
    m_sylar::Task<m_sylar::IOState> recvResponseFromBuffer(Response& frame, int request_id);        // 不涉及网络，从各种缓冲中获得数据尝试解析
    m_sylar::Task<m_sylar::IOState> recvRequestFromBuffer(Request& frame);                          // 不涉及网络，从各种缓冲中获得数据尝试解析

private:
    SessionState m_state {SessionState::INITING};
    int node_id {-1};                                                   // 当前对端节点node_id，配置项中应从0开始
    std::atomic<int> m_request_id {0};                                               // 请求id,用于分离请求与响应

    std::shared_ptr<m_sylar::IPv4Address> m_address = std::make_shared<m_sylar::IPv4Address>();     // 对端地址

    std::map<int, std::shared_ptr<Response>> m_recved_frames;           // 记录所有接收的帧内容
    Parser::ptr m_parser{std::make_shared<Parser>()};
    m_sylar::CoMutex m_send_mutex;                                      // 锁写
    m_sylar::CoMutex m_recv_mutex;                                      // 锁读
    m_sylar::CoMutex m_resp_buffer_mutex;                               // 锁接收缓冲区
    m_sylar::CoMutex m_req_buffer_mutex;                                // 锁接收缓冲区
};

}
}
