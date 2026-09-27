#pragma once
#include <array>
#include <atomic>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
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

// repair: co_sendRequest / co_recvResponse 的返回值说明（沿用 m_sylar::IOState）。
//
//   粒度问题不在枚举，而在【填得不够细】：原来这两个函数实际只会产生 SUCCESS / FAILED 两种
//     · co_sendRequest ：EPIPE 和普通错误都返回 FAILED，从不产生 TIMEOUT / CLOSED
//     · co_recvResponse：只有 recv_len==0 才返回 CLOSED，ETIMEDOUT 之外的连接错误
//                        （ECONNRESET / EPIPE）全被压成 FAILED
//   于是上层拿到 FAILED 时分不清是"连接断了"、"只是这次慢了"、还是"帧协议错位"。
//
//   现在按 IOState 已有的值填准：
//     SUCCESS —— 成功
//     TIMEOUT —— 等数据超时(errno=ETIMEDOUT)：对端可能只是慢，也可能半开连接
//     CLOSED  —— 连接/字节流已不可用，需要重建连接：
//                  recv 返回 0(EOF) / send 返回 0 / Socket 未连接(-2)
//                  EPIPE、ECONNRESET、ECONNABORTED、ENOTCONN、EBADF
//                  帧协议错位（长度头非法 / Parser BADFRAME / 解析零进展）
//     FAILED  —— 其它 I/O 错误（如 EIO）
//     UNKNOWN —— 内部状态异常（解析循环结束却拿不到帧）

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

    // repair: 删掉了 RPCSession::reConnect()。它已经没有任何调用点（重连改成在 RPCClient
    //   层整个替换 RPCSession 对象了），而且函数体用的是 getSocket()->close() ——
    //   m_sylar::Socket::close() 里 co_close(fd) 是被注释掉的，只注销 epoll 事件不真关 fd，
    //   留着会误导。要重连请用 RPCClient::coConnect(id)。

    // 对当前节点调用方法
    m_sylar::Task<m_sylar::IOState> call(std::shared_ptr<std::string> service,   // 服务
                                  std::shared_ptr<std::string> method,    // 方法
                                  std::shared_ptr<std::string> req_bytes,
                                  std::shared_ptr<std::string> resp_bytes);

    // 返回值仍是 m_sylar::IOState，只是填得更细（见文件开头说明）
    m_sylar::Task<m_sylar::IOState> co_sendRequest(const Frame& frame);
    m_sylar::Task<m_sylar::IOState> co_recvResponse(Frame& frame);

    // repair: 原来 node_id 恒为 0 且没有任何赋值入口，日志里永远是 "Peer 0"，排查时会误导
    void setNodeId(int id) { node_id = id; }
    [[nodiscard]] int getNodeId() const { return node_id; }

    void setAddress(const m_sylar::IPv4Address& address) const;

private:
    SessionState m_state {SessionState::INITING};
    int node_id {-1};            // repair: 原来初始化为 0（0 是合法 id，无法区分"未设置"）
    int m_request_id {0};        // 请求id,用于分离请求与响应

    std::shared_ptr<m_sylar::IPv4Address> m_address = std::make_shared<m_sylar::IPv4Address>();     // 对端地址

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
    m_sylar::Task<int> coConnectAll();      // 连接或重连
    m_sylar::Task<void, m_sylar::TaskBeginExecuter> coConnectTask(int id);
    m_sylar::Task<int> coConnect(int id );

    /**
     *  @brief 加一个 peer（配置装载用），返回它的 node id（= 加入顺序，从 0 开始）
     *  repair: 原来 m_infos / m_sessions 没有任何填充入口（init() 里的配置装载一直是 TODO），
     *     导致 RPCClient 这一层完全没法被驱动、更没法测。这里补上最小入口。
     *     注意 m_infos 和 m_sessions 必须同步增长、下标一一对应 —— coConnect / call 都按同一个下标访问。
     *     只在初始化阶段调用（运行期不再增删 peer）。
     */
    int addPeer(const m_sylar::IPAddress::ptr& address);
    /**
     *  @brief 远程RPC调用
     */
    m_sylar::Task<CallState> call(int node_id,
                                  std::shared_ptr<std::string> service,   // 服务
                                  std::shared_ptr<std::string> method,    // 方法
                                  std::shared_ptr<std::string> req_bytes,
                                  std::shared_ptr<std::string> resp_bytes);


class SessionInfo {
public:
    m_sylar::IPAddress::ptr address;
    std::atomic<bool> state{false};                 // false 未连接/已断开，true 已连接
    std::atomic<bool> reconnecting{false};          // 是否有协程正在重连本节点
    std::atomic<int>  consecutive_timeouts{0};      // 连续超时次数，成功即归零
};
private:
    State m_state{State::INIT};              // 当前客户端状态

    std::vector<std::unique_ptr<SessionInfo>> m_infos;
    std::vector<RPCSession::ptr> m_sessions;
    std::mutex m_sessions_mutex;
};

}  // namespace RPC
}  // namespace craft
