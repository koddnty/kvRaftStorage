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
#include "rpcConfig.hpp"
#include "rpcSession.hpp"

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

// 服务端 code → 客户端 CallState 的映射
static CallState codeToState(int code) {
    switch (static_cast<RpcCode>(code)) {
    case RpcCode::OK:          return CallState::SUCCESS;
    case RpcCode::UNK_SERVICE: return CallState::UNK_SERVICE;
    case RpcCode::UNK_METHOD:  return CallState::UNK_METHOD;
    case RpcCode::BAD_REQUEST: return CallState::UNK_REQBYTES;
    default:                   return CallState::FAILED;
    }
}

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





// RPC客户端
class RPCClient : public std::enable_shared_from_this<RPCClient>, Noncopyable {
public:
    using ptr = std::shared_ptr<RPCClient>;

    enum class State {
        INIT,
        READY,
        STOP,
        ERROR
    };

    /**
     * @brief 初始化客户端：读配置文件 → 反序列化 → 建连
     *
     *   走的就是 m_sylar 日志模块那一套（ConfigManager::LoadJson +
     *   ConfigManager::LookUp<RpcDefine> + FormatConversion<nlohmann::json, RpcDefine>），
     *   细节和两个坑都写在 rpcConfig.hpp 里了。
     *
     *   配置里的 nodes 是【集群全部节点，含自己】：除了 selfId 那个，其余每个都会
     *   addPeer() 建一条连接。selfId 通常来自命令行（-i <selfId>），
     *   三个节点因此可以共用同一份配置文件。
     *
     * @param confPath  配置文件路径，空串表示不读文件（此时 peers 为空，必然失败）
     * @param configId  该文件对应的 config_id（见 kRpcConfId 的说明）
     * @param selfId    自己在这个集群里的 id；<0 表示不指定（会把所有节点都当对端，含自己）
     * @return 成功返回 peer 数量（>= 0），配置错误返回 -1
     */
    m_sylar::Task<int> init(const RPC::RpcDefine& define, int selfId);
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
                                  const std::string service,   // 服务
                                  const std::string method,    // 方法
                                  std::shared_ptr<std::string> req_bytes,
                                  std::shared_ptr<std::string> resp_bytes);

    m_sylar::Task<void> stop();

    State getState() {return m_state; }

class SessionInfo {
public:
    m_sylar::IPAddress::ptr address;
    std::atomic<bool> state{false};                 // false 未连接/已断开，true 已连接
    std::atomic<bool> reconnecting{false};          // 是否有协程正在重连本节点
    std::atomic<int>  consecutive_timeouts{0};      // 连续超时次数，成功即归零
    // repair: 只给日志节流用 —— 连续重连失败轮数，连上就清零。
    //   定时器每 300ms 一轮，每轮对这个 peer 打一条 "reconnect 放弃" WARN，
    //   一个 down 掉的 peer 就是 ~3 条/秒；三个节点里两个 down 能到 ~7 条/秒，日志没法看。
    std::atomic<int>  fail_rounds{0};
};
private:
    std::atomic<State> m_state{State::INIT};              // 当前客户端状态

    std::vector<std::unique_ptr<SessionInfo>> m_infos;
    std::vector<RPCSession::ptr> m_sessions;
    std::mutex m_sessions_mutex;
};

}  // namespace RPC
}  // namespace craft
