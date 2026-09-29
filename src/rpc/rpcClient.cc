#include "rpcClient.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <utility>

#include <sylar/basic/log.h>
#include "payload.pb.h"

namespace craft {
namespace RPC {

static auto g_logger = M_SYLAR_LOG_NAME("craft");



// ---------------------------------------------------------------- RPCTask

Request RPCTask::toRequest() {
    Request req;
    req.set_id(m_request_id);
    if (m_service) req.set_service(*m_service);
    if (m_method) req.set_method(*m_method);
    if (m_req_bytes) req.set_data(*m_req_bytes);
    return req;
}

Response RPCTask::toResponse() {
    Response resp;
    resp.set_id(m_request_id);
    resp.set_code(0);
    if (m_resp_bytes) resp.set_data(*m_resp_bytes);
    return resp;
}




// ---------------------------------------------------------------- RPCClient

int RPCClient::addPeer(const m_sylar::IPAddress::ptr& address) {
    // repair: 补上配置装载的入口 —— 原来 m_infos / m_sessions 一个填充的地方都没有，
    //   RPCClient 这层等于没法用。m_infos 装 unique_ptr（SessionInfo 里有 atomic，
    //   既不可拷贝也不可移动，vector 直接装不下），m_sessions 必须同步增长。
    auto info = std::make_unique<SessionInfo>();
    info->address = address;
    const int id = static_cast<int>(m_infos.size());
    {   // 只在改 m_sessions 那一瞬间加锁（call() 会并发读它）
        std::lock_guard<std::mutex> lk(m_sessions_mutex);
        m_infos.push_back(std::move(info));
        m_sessions.emplace_back(nullptr);       // 新槽位 = 还没连上
    }
    return id;
}


m_sylar::Task<int> RPCClient::init(const RPC::RpcDefine& define, int selfId) {
    // 从配置装载所有连接信息

    if (selfId >= 0 && define.find(selfId) == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] selfId=" << selfId << " 不在配置的节点列表里";
        co_return -1;
    }
    if (selfId < 0) {
        M_SYLAR_LOG_WARN(g_logger) << "[rpc] 没有指定 selfId，会把配置里所有节点都当对端建连"
                                      "（含自己！正常应该传 -i <selfId>）";
    }

    // 重复 init 幂等：先把上一轮的清掉，再按下标重建
    m_infos.clear();
    {
        std::lock_guard<std::mutex> lk(m_sessions_mutex);
        m_sessions.clear();
    }

    // 全部节点里把自己摘出去 —— 自己不跟自己建 rpc 连接
    for (const auto& n : define.nodes) {
        if (n.id == selfId) {
            continue;
        }
        m_sylar::IPAddress::ptr addr = toAddress(n);
        if (addr == nullptr) {      // ip 非法（getaddrinfo 解析不出来）
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] 节点 " << n.id << " 的地址非法: " << n.ip;
            co_return -1;
        }
        const int peerId = addPeer(addr);
        M_SYLAR_LOG_INFO(g_logger) << "[rpc] peer[" << peerId << "] node=" << n.id
                                   << " -> " << n.addr();
    }

    // 尝试连接
    co_await coConnectAll();

    // 启动循环重连定时器
    auto client = shared_from_this();
    m_sylar::TimeTask::ptr task = m_sylar::TimeTask::create(300, true,
        [client](m_sylar::TimeTask::ptr timer) ->m_sylar::Task<void> {
            if (client->getState() == State::STOP) {
                timer->cancel();
            }
            else {
                co_await client->coConnectAll(); // 重连所有，虽阻塞但定时器实现是当前任务结束后才开始循环计时，应无问题
            }
        }
    );
    m_sylar::TimeManager::getInstance()->addTimer(task);

    m_state = State::READY;
    co_return static_cast<int>(m_infos.size());
}



m_sylar::Task<int> RPCClient::coConnectAll() {
    int started = 0;

    for (size_t i = 0; i < m_infos.size() && i < m_sessions.size(); ++i) {
        if (m_infos[i]->state.load(std::memory_order_acquire)) {
            continue;                                   // 已连接，不用动
        }
        co_await coConnect(i);  // 连接
        ++started;
    }
    co_return started;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> RPCClient::coConnectTask(int id ) {
    // 连接不正常，直接重新替换
    co_await coConnect(id);
    co_return;
}


m_sylar::Task<int> RPCClient::coConnect(int id ) {
    if (id < 0 || static_cast<size_t>(id) >= m_infos.size()
                || static_cast<size_t>(id) >= m_sessions.size()) {
        co_return -1;
    }
    SessionInfo& info = *m_infos[id];

    if (info.state.load(std::memory_order_acquire)) {
        co_return 0;                                    // 已经连上了，不用重连
    }
    // CAS 独占：只有抢到重连权的那个协程才往下走，避免同一节点被并发重连出多条连接
    bool expected = false;
    if (!info.reconnecting.compare_exchange_strong(expected, true)) {
        co_return 0;                                    // 已经有协程在重连这个节点了
    }
    // 不管从哪条路径返回都要放开重连权
    struct ReconnectingGuard {
        std::atomic<bool>& flag;
        ~ReconnectingGuard() { flag.store(false); }
    } guard{info.reconnecting};

    // 这几个从配置读一次就固定下来，免得循环中途配置被改掉、退避序列变得不可预期
    const int maxAttempts = kReconnectMaxAttempts();
    const unsigned int maxBackoffMs = kReconnectMaxBackoffMs();
    unsigned int backoff_ms = kReconnectBackoffMs();
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        // 重建连接
        m_sylar::Socket::ptr sock = m_sylar::Socket::CreateTCP(info.address);
        int rt = co_await sock->connect(info.address, kRpcTimeoutUs());
        if (rt == 0) {
            // 连接成功：整个替换掉旧的 RPCSession（旧对象由 shared_ptr 自然回收）
            auto session = std::make_shared<RPCSession>(sock);
            session->setNodeId(id);
            // recv/send 超时 RPCSession 的构造函数里已经定死了（见上面那条 repair），这里不重复设置
            {   // 只在替换那一瞬间加锁，临界区里没有任何挂起点
                std::lock_guard<std::mutex> lk(m_sessions_mutex);
                m_sessions[id] = session;
            }
            info.consecutive_timeouts.store(0);
            info.fail_rounds.store(0);      // repair: 连上了就把失败轮数清零，日志节流重新开始
            info.state.store(true, std::memory_order_release);
            co_return 0;
        }
        if (attempt + 1 < maxAttempts) {
            co_await m_sylar::co_sleep(backoff_ms);
            backoff_ms = std::min(backoff_ms * 2, maxBackoffMs);
        }
    }

    // 放弃本轮。state 保持 false：call() 会立刻返回 RETRY 快速失败，不再白等一次 recv 超时；
    // 真正的重试交给周期性的 coConnectAll()。
    // repair: 日志节流 —— 定时器每 300ms 一轮，每轮都打 WARN 的话一个 down peer 就是 ~3 条/秒。
    //   改成：第 1 轮打（让你知道它断了），之后每 20 轮（≈6 秒）才打一次，中间静默。
    //   注意这里【只】节流日志，重连本身照旧每 300ms 试一次 —— 退避是【不能】做的，见下面的说明。
    const int rounds = info.fail_rounds.fetch_add(1) + 1;
    if (rounds == 1 || rounds % 20 == 0) {
        M_SYLAR_LOG_WARN(g_logger) << "[rpc] reconnect 放弃, peer " << id
                                   << ", 尝试 " << maxAttempts << " 次均失败，已连续 " << rounds << " 轮";
    }
    co_return -1;
}


m_sylar::Task<CallState> RPCClient::call(int node_id,
                                         const std::string service,   // 服务
                                         const std::string method,    // 方法
                                         std::shared_ptr<std::string> req_bytes,
                                         std::shared_ptr<std::string> resp_bytes) {
    if (m_state != State::READY) {co_return CallState::FAILED; }
    if (node_id < 0 || static_cast<size_t>(node_id) >= m_sessions.size()
                    || static_cast<size_t>(node_id) >= m_infos.size()) {
        M_SYLAR_LOG_WARN(g_logger) << "invalid node id = " << node_id << ", peer number = " << m_sessions.size();
        co_return CallState::UNK_NODE;
    }

    SessionInfo& info = *m_infos[node_id];

    if (!info.state.load(std::memory_order_acquire)) {
        co_return CallState::RETRY;
    }

    RPCSession::ptr session;
    {   // 只在取 shared_ptr 那一瞬间加锁：重连协程会整体替换 m_sessions[node_id]
        std::lock_guard<std::mutex> lk(m_sessions_mutex);
        session = m_sessions[node_id];
    }
    if (session == nullptr) {
        // repair: 原来这里返回 UNK_NODE —— 节点是认识的，只是还没握手完，语义不对。
        //   上层靠 UNK_NODE 判断"配置里没有这个节点"，混进来一个"还没连上"会误导排查。
        co_return CallState::RETRY;
    }

    m_sylar::IOState st =  co_await session->call(service, method, std::move(req_bytes),
                                              std::move(resp_bytes));
    if (st == m_sylar::IOState::TIMEOUT) {

        const int n = info.consecutive_timeouts.fetch_add(1) + 1;
        if (n >= kTimeoutToDisconnectCount()) {
            st = m_sylar::IOState::CLOSED;      // 连续超时过多，解释为断开
        }
    }

    if (st == m_sylar::IOState::CLOSED) {

        bool expected = true;
        if (info.state.compare_exchange_strong(expected, false)) {
            info.consecutive_timeouts.store(0);
            m_sylar::IOManager::getInstance()->schedule(
                m_sylar::TaskCoro20::create_coro(
                    std::bind(&RPCClient::coConnectTask, this, node_id)));
        }
        // repair: 用 RETRY 而不是 FAILED —— 这是链路问题（对端断了/在重连），不是对端返回了错误。
        //   CallState::RETRY 这个枚举值原来从来没被返回过。
        co_return CallState::RETRY;
    }

    if (st == m_sylar::IOState::SUCCESS) {
        info.consecutive_timeouts.store(0);     // 连续计数：一次成功就清零
        co_return CallState::SUCCESS;
    }


    if (st == m_sylar::IOState::TIMEOUT) {
        co_return CallState::TIMEOUT;
    }
    co_return CallState::FAILED;                // 其它 I/O 错误（FAILED / UNKNOWN）
}


m_sylar::Task<void> RPCClient::stop() {
    m_state = State::STOP;
    co_return;      // repair: 原来没有 co_return —— 声明返回 Task<void> 却不是协程，
                    //   编译期报 "no return statement in function returning non-void"，
                    //   运行期返回的是垃圾 Task。补上让它成为真正的协程。
}
}  // namespace RPC
}  // namespace craft
