#pragma once
#include <array>
#include <atomic>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include <sylar/basic/noncopyable.h>
#include <sylar/coroutine/corobase.h>
#include <sylar/server/tcp/tcpServer.h>
#include <sylar/server/common/session.hpp>
#include "frame.hpp"
#include "parser.hpp"
#include "payload.pb.h"
#include <sylar/basic/lock.hpp>

/**
 * 实现rpc客户端
 *  需要注意的是，此类禁止拷贝
 */
namespace craft {
namespace RPC {

// 数据定义
enum class CallState {
    UNK_NODE,
    UNK_SERVICE,
    UNK_METHOD,
    UNK_REQBYTES,
    RETRY,
    FAILED,
    TIMEOUT,
    SUCCESS
};      // call响应状态

// repair: 服务端响应码，对应 Request/Response 的 code 字段。
//         原来客户端从不检查 code → 服务端的错误（无此 service / method）传不回客户端。
//         服务端 RPCServer 实现时也用这套值，两边必须一致。
enum class RpcCode : int {
    OK          = 0,        // 成功
    BAD_REQUEST = 1001,     // 请求体解析失败
    UNK_SERVICE = 1002,     // 服务端没有注册这个 service
    UNK_METHOD  = 1003,     // 该 service 下没有这个 method
    INTERNAL    = 1004,     // 服务端处理时内部错误
};

enum class SessionState {
    INITING,        // 没初始化
    READY,          // 可用
    BUSY,           // 被占用，繁忙
    OFFLINE,        // 离线（超时，退出等状态）
    ERROR           // 发生错误
};      // 节点状态

// 一个rpc调用任务
class RPCTask : public std::enable_shared_from_this<RPCTask> {
public:
    using ptr = std::shared_ptr<RPCTask>;

    RPCTask(int request_id, std::shared_ptr<std::string> service, std::shared_ptr<std::string> method,
            std::shared_ptr<std::string> req_bytes, std::shared_ptr<std::string> resp_bytes)
        : m_request_id(request_id),
          m_service(std::move(service)),
          m_method(std::move(method)),
          m_req_bytes(std::move(req_bytes)),
          m_resp_bytes(std::move(resp_bytes)) {}

    Request toRequest();            // 制作请求
    Response toResponse();          // 生成响应

    int m_request_id{0};
    std::shared_ptr<std::string> m_service;           // 服务名
    std::shared_ptr<std::string> m_method;            // 方法名
    std::shared_ptr<std::string> m_req_bytes;         // 请求参数
    std::shared_ptr<std::string> m_resp_bytes;        // 响应

    // ---- 完成状态：driver 填，waiter 读 ----
    std::atomic<bool> m_done{false};
    CallState m_result{CallState::RETRY};
    std::function<void(CallState)> m_cb;              // waiter 挂起时注册，由 driver 唤醒
};

// 前向声明
class RPCClient;

class RPCSession : public m_sylar::Session {
public:
    using ptr = std::shared_ptr<RPCSession>;
    RPCSession(m_sylar::Socket::ptr sock);
    ~RPCSession();


    // 对当前节点调用方法
    m_sylar::Task<CallState> call(std::shared_ptr<std::string> service,   // 服务
                                  std::shared_ptr<std::string> method,    // 方法
                                  std::shared_ptr<std::string> req_bytes,
                                  std::shared_ptr<std::string> resp_bytes);

    m_sylar::Task<m_sylar::IOState> co_sendRequest(const Frame& frame);
    m_sylar::Task<m_sylar::IOState> co_recvResponse(Frame& frame);

    // repair: 原来 node_id 恒为 0 且没有任何赋值入口，日志里永远是 "Peer 0"，排查时会误导
    void setNodeId(int id) { node_id = id; }
    int  getNodeId() const { return node_id; }

private:
    SessionState m_state {SessionState::INITING};
    int node_id {-1};            // repair: 原来初始化为 0（0 是合法 id，无法区分"未设置"）
    int m_request_id {0};        // 请求id,用于分离请求与响应

    Parser::ptr m_parser{std::make_shared<Parser>()};
    m_sylar::CoMutex m_mutex;
};




// RPC客户端
class RPCClient : public std::enable_shared_from_this<RPCClient>, Noncopyable {
public:
    using ptr = std::shared_ptr<RPCClient>;

    enum class State {
        INIT,
        READY,
        ERROR
    };

    /**
     * @brief 初始化客户端
     */
    m_sylar::Task<int> init();

    /**
     *  @brief 远程RPC调用
     */
    m_sylar::Task<CallState> call(int node_id,
                                  std::shared_ptr<std::string> service,   // 服务
                                  std::shared_ptr<std::string> method,    // 方法
                                  std::shared_ptr<std::string> req_bytes,
                                  std::shared_ptr<std::string> resp_bytes);

private:
    State m_state{State::INIT};              // 当前客户端状态
    std::vector<RPCSession::ptr> m_sessions;
};

}  // namespace RPC
}  // namespace craft
