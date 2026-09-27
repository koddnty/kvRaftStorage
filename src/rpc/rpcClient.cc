#include "rpcClient.hpp"

#include <cerrno>
#include <cstring>
#include <utility>

#include <sylar/basic/log.h>
#include "payload.pb.h"

namespace craft {
namespace RPC {

static auto g_logger = M_SYLAR_LOG_NAME("craft");

// RPC 调用超时。Raft 心跳 25ms、选举超时 300~500ms，
// 所以取 100ms：既远小于选举超时，又给慢对端留了余量。
static constexpr int kRpcTimeoutUs = 100 * 1000;

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
    setRecvTimeOut(kRpcTimeoutUs);
    setSendTimeOut(kRpcTimeoutUs);
    m_state = SessionState::READY;
}


RPCSession::~RPCSession() = default;


m_sylar::Task<CallState> RPCSession::call(std::shared_ptr<std::string> service,   // 服务
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
        co_return CallState::FAILED;
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
    if (m_sylar::IOState::SUCCESS != co_await co_sendRequest(send_frame)) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] failed to send request to peer " << node_id;
        co_return CallState::FAILED;
    }

    Frame resp_frame;
    std::shared_ptr<Response> resp = std::make_shared<Response>();
retry:
    const m_sylar::IOState io = co_await co_recvResponse(resp_frame);        // 接收消息
    if (m_sylar::IOState::SUCCESS != io) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] failed to recv response from peer " << node_id;
        // repair: 把 IOState 的 TIMEOUT 透出来，别一律压成 FAILED
        co_return (io == m_sylar::IOState::TIMEOUT) ? CallState::TIMEOUT : CallState::FAILED;
    }
    if (false == resp->ParseFromString(resp_frame.payload())) {     // 解析帧
        M_SYLAR_LOG_ERROR(g_logger) << "failed to parser rpc frame.";
        co_return CallState::FAILED;
    }
    if (resp->id() != request_id) {     // 消息所属检查
        M_SYLAR_LOG_WARN(g_logger) <<   "recved frame dose not match with sended frame, " <<
                                        "request id : " << request_id << " recv id : " << resp->id();
        goto retry;     // 帧不对，丢弃
    }

    // repair: 原来不检查 resp->code() —— 服务端的错误（无此 service / method）会被当成成功返回
    if (resp->code() != static_cast<int>(RpcCode::OK)) {
        M_SYLAR_LOG_WARN(g_logger) << "[rpc] peer " << node_id << " returned code=" << resp->code()
                                   << " errmsg={" << resp->errmsg() << "} service={" << *service
                                   << "} method={" << *method << "}";
        co_return codeToState(resp->code());
    }

    // repair: resp_bytes 是出参，传 nullptr 会让调用方在 *resp_bytes 处崩
    if (resp_bytes != nullptr) {
        *resp_bytes = resp->data();
    }
    co_return CallState::SUCCESS;
}


// 不加锁
m_sylar::Task<m_sylar::IOState> RPCSession::co_sendRequest(const Frame&  frame) {
    const std::string buffer = frame.toWire();
    const size_t total_size = buffer.length();
    size_t offset = 0;
    while(offset < total_size)
    {
        const int send_size = co_await sendMessage(buffer.c_str() + offset, total_size - offset);
        if(send_size == -1 && errno == EPIPE)
        {
            co_return m_sylar::IOState::FAILED;
        }
        if(send_size == -1)
        {
            M_SYLAR_LOG_WARN(g_logger) << "rpc frame send failed, errno:" << errno << " error:" << strerror(errno);
            co_return m_sylar::IOState::FAILED;
        }
        offset += send_size;
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
    size_t max_request_size = kMaxPayloadSize;
    char* buffer = nullptr;     // 输出参数，指向接收数据的起始位置，不要对buffer进行delete操作
    size_t total_length = 0;      // 累计接收字节数
    while(true) {
        // 接受缓冲区信息
        const int recv_len = co_await recvMessage(&buffer, -1);
        // 接收错误处理
        if(recv_len == 0)
        {   // 连接关闭
            co_return m_sylar::IOState::CLOSED;
        }
        else if(recv_len < 0)
        {   // 错误
            // repair: 区分超时和其它错误。Raft 需要知道对端是"慢"（TIMEOUT，可以再等等）
            //         还是"挂了"（FAILED，该触发重连/退避）—— 原来两者都返回 FAILED。
            if (errno == ETIMEDOUT) {
                M_SYLAR_LOG_DEBUG(g_logger) << "[rpc] recv timeout, peer " << node_id
                                            << " errno=ETIMEDOUT";
                co_return m_sylar::IOState::TIMEOUT;
            }
            M_SYLAR_LOG_WARN(g_logger) << "recv http request failed"
                                        << ", errno:" << errno
                                        << " error:" << strerror(errno);
            co_return m_sylar::IOState::FAILED;
        }

        // 接收信息处理
        size_t parsed = 0;
        try{
            parsed = m_parser->parse(buffer, recv_len);
        } catch (std::exception &e) {
            // repair: 原来用 std::cout 打异常，和项目其它地方统一用 logger 不一致
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser threw: " << e.what() << ", peer " << node_id;
            co_return m_sylar::IOState::FAILED;
        }
        consume(parsed);        // 消耗buffer中的部分内容
        total_length += parsed;

        // repair: 零进展 / 坏帧的死循环保护。
        //   帧损坏时 Parser 进入 BADFRAME 会提前返回（parsed < recv_len），
        //   consume 只吃掉一部分，下一轮 recvMessage 又把剩下的原样返回，
        //   parse 立刻返回 0 → consume(0) → total_length 不再增长 → 死循环；
        //   而且下面的 max_request_size 检查因为 total_length 不动而永远不触发。
        if (m_parser->hasError()) {
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser in BADFRAME state, closing peer " << node_id;
            co_return m_sylar::IOState::FAILED;
        }
        if (parsed == 0 && recv_len > 0) {
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser made no progress, recv_len=" << recv_len
                                        << ", closing peer " << node_id;
            co_return m_sylar::IOState::FAILED;
        }

        // 解析状态处理
        if(total_length > max_request_size)
        {
            M_SYLAR_LOG_WARN(g_logger) << "[rpc] message is too long, total length=" << total_length << " max length=" << max_request_size;
            co_return m_sylar::IOState::FAILED;
        }

        if(!m_parser->empty()) {
            // 解析完成
            break;
        }
    }

    // resp处理
    // repair: popFrame() 可能返回 nullptr，原来直接解引用是 UB，这里补检查
    Frame::ptr done = m_parser->popFrame();
    if (done == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] no frame after parse loop, peer " << node_id;
        co_return m_sylar::IOState::FAILED;
    }
    frame = std::move(*done);

    co_return m_sylar::IOState::SUCCESS;
}


// ---------------------------------------------------------------- RPCClient

m_sylar::Task<int> RPCClient::init() {
    // 从配置装载所有连接信息

    // 尝试连接
    // 设置保活 Ping 定时器、重试定时器
    for (size_t i = 0; i < m_sessions.size(); ++i) {
        RPCSession::ptr& session = m_sessions[i];
        if (session == nullptr || session->getSocket() == nullptr) {
            continue;
        }
        // repair: 把下标写回 session，否则它的日志永远打印 "Peer -1"
        session->setNodeId(static_cast<int>(i));
        // 超时设置（RPCSession 构造里已设过一次，这里再兜一次：init 可能在 session
        // 建好之后才被调用，也可能有人中途改过超时）
        session->getSocket()->setRecvTimeOut(kRpcTimeoutUs);
        session->getSocket()->setSendTimeOut(kRpcTimeoutUs);
    }
    m_state = State::READY;
    co_return 0;
}


m_sylar::Task<CallState> RPCClient::call(int node_id,
                                         std::shared_ptr<std::string> service,   // 服务
                                         std::shared_ptr<std::string> method,    // 方法
                                         std::shared_ptr<std::string> req_bytes,
                                         std::shared_ptr<std::string> resp_bytes) {
    if (node_id < 0 || static_cast<size_t>(node_id) >= m_sessions.size() || m_sessions[node_id] == nullptr) {
        M_SYLAR_LOG_WARN(g_logger) << "invalid node id = " << node_id << ", peer number = " << m_sessions.size();
        co_return CallState::UNK_NODE;
    }
    co_return co_await m_sessions[node_id]->call(std::move(service), std::move(method), std::move(req_bytes),
                                              std::move(resp_bytes));
}

}  // namespace RPC
}  // namespace craft
