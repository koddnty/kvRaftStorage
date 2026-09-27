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


// repair: 服务端 code → 客户端 CallState 的映射（原来完全没做这层）
static CallState codeToState(int code) {
    switch (static_cast<RpcCode>(code)) {
        case RpcCode::OK:          return CallState::SUCCESS;
        case RpcCode::UNK_SERVICE: return CallState::UNK_SERVICE;
        case RpcCode::UNK_METHOD:  return CallState::UNK_METHOD;
        case RpcCode::BAD_REQUEST: return CallState::UNK_REQBYTES;
        default:                   return CallState::FAILED;
    }
}

// repair: errno → m_sylar::IOState。co_sendRequest / co_recvResponse 共用这一套归类。
//   关键是把【连接确实断了】和【只是这次慢了】分开 —— 两者的处置相反：
//     断了的必须重建连接；慢的不该乱断（可能只是网络抖动，重连还要付一次握手）。
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



// RPC Session ----------------------------------------------------------------
RPCSession::RPCSession(m_sylar::Socket::ptr sock) : m_sylar::Session(sock) {
    // repair: 基类 Session 的构造函数会无条件把超时覆盖成配置默认值
    //         （servers.basic.timeout.recv，默认 30 秒）。30s 的 recv 超时对
    //         25ms 心跳 / 300~500ms 选举超时的 Raft 是灾难，这里显式定死。
    // repair: ★ 不要调 setBufferSize()！
    //   m_sylar::Session::setBufferSize() 只改 m_buffer_size 这个【逻辑值】，
    //   并不会重新分配缓冲区（构造函数里是 m_buffer = new char[配置值]，默认 1024）。
    //   一旦把逻辑值改大，recvMessage 就会往 1024 字节的堆块里写更多字节 → 堆溢出（实测段错误）。
    //   要放大 buffer 必须在【任何 Session 构造之前】改配置项
    //   servers.basic.limit.buffer_size（见本文件顶部的静态初始化）。
    setRecvTimeOut(kRpcTimeoutUs());
    setSendTimeOut(kRpcTimeoutUs());
    m_state = SessionState::READY;
}


RPCSession::~RPCSession() = default;


m_sylar::Task<m_sylar::IOState> RPCSession::call(std::shared_ptr<std::string> service,   // 服务
                              std::shared_ptr<std::string> method,    // 方法
                              std::shared_ptr<std::string> req_bytes,
                              std::shared_ptr<std::string> resp_bytes) {
    // 加锁
    m_sylar::CoUniqueLock lock(m_mutex);
    co_await lock.lock();
    // 状态检查
    const SessionState cur = m_state;
    if (cur != SessionState::READY && cur != SessionState::BUSY) {
        M_SYLAR_LOG_ERROR(g_logger) << "Peer " << node_id << " is not ready, state = "
                                    << static_cast<int>(cur);
        co_return m_sylar::IOState::FAILED;
    }


    // 数据准备
    Request req;
    int request_id = m_request_id++;
    req.set_service(*service);
    req.set_method(*method);
    req.set_data(*req_bytes);
    req.set_id(request_id);
    Frame send_frame;
    send_frame.setData(req.SerializeAsString());

    // repair: 原来忽略了 co_sendRequest 的返回值 —— 发送失败后还要干等一次 recv 超时
    //         （100ms）才报错。对 25ms 心跳的 Raft 来说这 100ms 是纯浪费。
    const m_sylar::IOState send_rt = co_await co_sendRequest(send_frame);
    if (send_rt != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] send 失败, peer " << node_id
                                    << " io=" << ioStateName(send_rt);
        co_return send_rt;
    }

    Frame resp_frame;
    std::shared_ptr<Response> resp = std::make_shared<Response>();
retry:
    const m_sylar::IOState recv_rt = co_await co_recvResponse(resp_frame);        // 接收消息
    if (recv_rt != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] recv 失败, peer " << node_id
                                    << " io=" << ioStateName(recv_rt);

        co_return recv_rt;
    }
    if (false == resp->ParseFromString(resp_frame.payload())) {     // 解析帧
        M_SYLAR_LOG_ERROR(g_logger) << "failed to parser rpc frame.";
        co_return m_sylar::IOState::FAILED;     // 可能的消息错乱。返回failed,触发重连
    }
    if (resp->id() != request_id) {     // 消息所属检查
        M_SYLAR_LOG_WARN(g_logger) <<   "recved frame dose not match with sended frame, " <<
                                        "request id : " << request_id << " recv id : " << resp->id();
        goto retry;     // 帧不对，丢弃
    }


    // repair: resp_bytes 是出参，传 nullptr 会让调用方在 *resp_bytes 处崩
    if (resp_bytes != nullptr) {
        *resp_bytes = resp->data();
    }
    co_return m_sylar::IOState::SUCCESS;
}


// 不加锁
m_sylar::Task<m_sylar::IOState> RPCSession::co_sendRequest(const Frame& frame) {
    const std::string buffer = frame.toWire();
    const size_t total_size = buffer.size();
    size_t offset = 0;
    while (offset < total_size)
    {
        const int send_size = co_await sendMessage(buffer.c_str() + offset, total_size - offset);
        if (send_size > 0) {
            offset += static_cast<size_t>(send_size);
            continue;
        }

        // repair: 原来是 `if (EPIPE) return FAILED; if (-1) return FAILED;`
        //   —— 两个分支返回同一个值，等于没分类，而且从不产生 TIMEOUT / CLOSED。
        //   现在按 errno 归类，并单独处理 Socket 的 0 / -2 约定：
        //     send_size == -1      -> 看 errno（ETIMEDOUT -> TIMEOUT，连接类错误 -> CLOSED）
        //     send_size == 0 或 -2 -> 连接不可用 -> CLOSED
        //                            （-2 是 Socket::send 的"未连接"约定；0 表示对端已关）
        const int err = errno;
        const m_sylar::IOState rt =
            (send_size == -1) ? errnoToIOState(err) : m_sylar::IOState::CLOSED;
        M_SYLAR_LOG_WARN(g_logger) << "[rpc] send failed, peer " << node_id << " ret=" << send_size
                                   << " errno=" << err << "(" << strerror(err) << ")"
                                   << " sent=" << offset << "/" << total_size
                                   << " io=" << ioStateName(rt);
        co_return rt;
    }
    co_return m_sylar::IOState::SUCCESS;
}


// 不加锁
m_sylar::Task<m_sylar::IOState> RPCSession::co_recvResponse(Frame& frame) {
    // repair: 解析器里可能已经攒好了完整的帧。
    //   一次 recvMessage 可能读到多条响应，parse 会把它们全解出来放进 m_parsed_frames，
    //   但每次只 popFrame 一条，剩下的留在队列里。
    //   原来这里无条件先调 recvMessage，会为了那条"已经在手里"的帧白白挂起等新数据
    //   —— 表现为假超时，或要等到下一条响应到达才被顺带取走。
    if (!m_parser->empty()) {
        frame = std::move(*m_parser->popFrame());
        co_return m_sylar::IOState::SUCCESS;
    }

    // 数据接受并解析
    size_t max_request_size = kMaxPayloadSize();
    char* buffer = nullptr;     // 输出参数，指向接收数据的起始位置，不要对buffer进行delete操作
    size_t total_length = 0;      // 累计接收字节数
    while(true) {
        // 接受缓冲区信息
        const int recv_len = co_await recvMessage(&buffer, -1);
        // 接收错误处理
        if(recv_len == 0)
        {
            // repair: 对端正常关闭（EOF）—— 归 CLOSED，和"连接被重置"走同一类，
            //   日志里能看出是谁先关的（recv=0/EOF vs ECONNRESET）
            M_SYLAR_LOG_WARN(g_logger) << "[rpc] peer " << node_id << " 关闭了连接 (recv=0/EOF)";
            co_return m_sylar::IOState::CLOSED;
        }
        else if(recv_len < 0)
        {
            // repair: 原来只挑出 ETIMEDOUT，其余（含 ECONNRESET / EPIPE）全归 FAILED ——
            //   于是"连接确实断了"的信号被埋进普通错误里，上层没法决定要不要重连。
            //   现在统一走 errnoToIOState：
            //     ETIMEDOUT            -> TIMEOUT （可能只是慢，也可能是半开）
            //     ECONNRESET/EPIPE/... -> CLOSED  （连接确实断了）
            //     其它                 -> FAILED
            const int err = errno;
            const m_sylar::IOState rt = errnoToIOState(err);
            if (rt == m_sylar::IOState::TIMEOUT) {
                M_SYLAR_LOG_DEBUG(g_logger) << "[rpc] recv timeout, peer " << node_id
                                            << " recv_len=" << recv_len << " (ETIMEDOUT)";
            } else {
                M_SYLAR_LOG_WARN(g_logger) << "[rpc] recv failed, peer " << node_id
                                           << " recv_len=" << recv_len << " errno=" << err
                                           << "(" << strerror(err) << ")"
                                           << " io=" << ioStateName(rt);
            }
            co_return rt;
        }

        // 接收信息处理
        size_t parsed = 0;
        try{
            parsed = m_parser->parse(buffer, recv_len);
        } catch (std::exception &e) {
            // repair: 原来用 std::cout 打异常，和项目其它地方统一用 logger 不一致；
            //         返回值也从 FAILED 改成 CLOSED —— 解析器抛异常说明字节流已经错位
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser threw: " << e.what() << ", peer " << node_id;
            co_return m_sylar::IOState::CLOSED;
        }
        consume(parsed);        // 消耗buffer中的部分内容
        total_length += parsed;

        // repair: 零进展 / 坏帧的死循环保护。
        //   帧损坏时 Parser 进入 BADFRAME 会提前返回（parsed < recv_len），
        //   consume 只吃掉一部分，下一轮 recvMessage 又把剩下的原样返回，
        //   parse 立刻返回 0 → consume(0) → total_length 不再增长 → 死循环；
        //   而且下面的 max_request_size 检查因为 total_length 不动而永远不触发。
        //
        // repair: 这三种都归 CLOSED 而不是 FAILED ——
        //   连接没断，但这条字节流已经错位、不可恢复，必须重建连接才能继续。
        if (m_parser->hasError()) {
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser in BADFRAME state, peer " << node_id
                                        << " 流已错位";
            co_return m_sylar::IOState::CLOSED;
        }
        if (parsed == 0 && recv_len > 0) {
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser made no progress, recv_len=" << recv_len
                                        << ", peer " << node_id << " 流已错位";
            co_return m_sylar::IOState::CLOSED;
        }

        // 解析状态处理
        if(total_length > max_request_size)
        {
            M_SYLAR_LOG_WARN(g_logger) << "[rpc] message is too long, total=" << total_length
                                       << " max=" << max_request_size << ", peer " << node_id
                                       << " 长度头不可信";
            co_return m_sylar::IOState::CLOSED;
        }

        if(!m_parser->empty()) {
            // 解析完成
            break;
        }
    }

    // resp处理
    // repair: popFrame() 可能返回 nullptr，原来直接解引用是 UB，这里补检查；
    //         归 UNKNOWN（内部状态异常），和 I/O 错误分开
    Frame::ptr done = m_parser->popFrame();
    if (done == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] no frame after parse loop, peer " << node_id;
        co_return m_sylar::IOState::UNKNOWN;
    }
    frame = std::move(*done);

    co_return m_sylar::IOState::SUCCESS;
}


void RPCSession::setAddress(const m_sylar::IPv4Address& address) const {
    *m_address = address;
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


m_sylar::Task<int> RPCClient::init(const std::string& confPath, int configId, int selfId) {
    // 从配置装载所有连接信息
    //   配置机制、FormatConversion 的写法、以及"ConfigVar 构造即快照"和"setConfig 是覆盖
    //   不是合并"这两个坑，都在 rpcConfig.hpp 里说明了。
    RpcDefine def;
    if (loadRpcConfig(confPath, configId, def) != 0) {
        // def.errmsg 里已经是具体原因，loadRpcConfig 也打过日志了
        co_return -1;
    }

    if (selfId >= 0 && def.find(selfId) == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] selfId=" << selfId << " 不在配置的节点列表里（"
                                    << confPath << "）";
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
    for (const auto& n : def.nodes) {
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
    // repair: coConnectAll() 只是把重连协程 schedule 出去就返回了，并不等连接建立，
    //   所以这里的 READY 只表示"已发起连接"，不代表"已连上"。
    //   真正的可用性判断看 per-node 的 SessionInfo::state（call() 据此返回 RETRY）。
    m_state = State::READY;
    co_return static_cast<int>(m_infos.size());
}



m_sylar::Task<int> RPCClient::coConnectAll() {
    int started = 0;

    for (size_t i = 0; i < m_infos.size() && i < m_sessions.size(); ++i) {
        if (m_infos[i]->state.load(std::memory_order_acquire)) {
            continue;                                   // 已连接，不用动
        }
        m_sylar::IOManager::getInstance()->schedule(
            m_sylar::TaskCoro20::create_coro(std::bind(&RPCClient::coConnectTask, this, static_cast<int>(i))));
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
    // repair: 这个函数整体重写，对应三个问题：
    //   1) 原来从 `co_await lock.lock()` 起一直持有 m_infos[id].mutex 直到 co_return，
    //      中间跨 co_sleep / connect 挂起；而 RPCClient::call 的 CLOSED 分支要拿同一把锁，
    //      于是对端一直不回来时，对该节点的所有 call() 全部永久卡死 → Raft 心跳协程卡死
    //      → leader 被选掉。现在改用 per-node 的 atomic<bool> reconnecting 做 CAS 独占，
    //      全程不持任何锁。
    //   2) 原来的 `while (rt)` 没有尝试上限且 co_sleep(1) 是 1 毫秒，见上面常量处的说明。
    //   3) 原来的 co_sleep 在循环体末尾，成功那一次也会先白睡一下才退出循环；现在只在失败分支退避。
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
        // repair: 每次尝试都新建 socket。connect 失败的那个 socket 不可复用 ——
        //   框架里 connect 失败既不关 fd 也不重置 m_isConnected；这里靠下一轮的重新赋值
        //   把它析构掉（~Socket → IOManager::closeFd → CLOSE_TASK → co_close(fd)），fd 能收回来。
        m_sylar::Socket::ptr sock = m_sylar::Socket::CreateTCP(info.address);
        // repair: 这里传进去的 kRpcTimeoutUs() 其实【没被用到】—— m_sylar::Socket::connect
        //   (socket.cc:329) 直接 `co_connect(fd, addr, addrLen)`，把 timeOut 形参丢了。
        //   真正生效的是全局配置 servers.http.tcpserver.timeout.connect（默认 5000ms），
        //   所以对端 SYN 被 DROP 时这里会阻塞到那个值。要收紧只能在起服务前改那个配置项。
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
    M_SYLAR_LOG_WARN(g_logger) << "[rpc] reconnect 放弃, peer " << id
                               << ", 尝试 " << maxAttempts << " 次均失败";
    co_return -1;
}


m_sylar::Task<CallState> RPCClient::call(int node_id,
                                         std::shared_ptr<std::string> service,   // 服务
                                         std::shared_ptr<std::string> method,    // 方法
                                         std::shared_ptr<std::string> req_bytes,
                                         std::shared_ptr<std::string> resp_bytes) {
    if (node_id < 0 || static_cast<size_t>(node_id) >= m_sessions.size()
                    || static_cast<size_t>(node_id) >= m_infos.size()) {
        M_SYLAR_LOG_WARN(g_logger) << "invalid node id = " << node_id << ", peer number = " << m_sessions.size();
        co_return CallState::UNK_NODE;
    }

    SessionInfo& info = *m_infos[node_id];

    // repair: 未连接 / 断线重连中 → 立刻返回 RETRY 快速失败。两个作用：
    //   (a) 不再去白等一次 100ms 的 recv 超时（对 25ms 心跳的 Raft 来说这 100ms 是纯浪费）；
    //   (b) 关键 —— 不再在这里拿锁。原来 CLOSED 分支要先 co_await CoMutex::lock() 去拿
    //       m_infos[node_id].mutex，而重连协程持着那把锁跨挂起点，于是对端一直不回来时，
    //       对该节点的所有 call() 全部永久卡死（Raft 心跳协程一卡住，leader 直接被选掉）。
    //       现在只读一个 atomic<bool>，零锁、零阻塞。
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

    m_sylar::IOState st =  co_await session->call(std::move(service), std::move(method), std::move(req_bytes),
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

}  // namespace RPC
}  // namespace craft
