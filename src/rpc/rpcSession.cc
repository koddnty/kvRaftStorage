#include "rpcSession.hpp"

namespace craft {
namespace RPC {
    static auto g_logger = M_SYLAR_LOG_NAME("craft");




// RPC Session ----------------------------------------------------------------
RPCSession::RPCSession(m_sylar::Socket::ptr sock) : m_sylar::Session(sock) {
    setRecvTimeOut(kRpcTimeoutUs());
    setSendTimeOut(kRpcTimeoutUs());
    m_state = SessionState::READY;
}


RPCSession::~RPCSession() = default;


m_sylar::Task<m_sylar::IOState> RPCSession::call(std::shared_ptr<std::string> service,   // 服务
                              std::shared_ptr<std::string> method,    // 方法
                              std::shared_ptr<std::string> req_bytes,
                              std::shared_ptr<std::string> resp_bytes) {
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
    const m_sylar::IOState send_rt = co_await co_sendMessage(send_frame);
    if (send_rt != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] send 失败, peer " << node_id
                                    << " io=" << ioStateName(send_rt);
        co_return send_rt;
    }

    std::shared_ptr<Response> resp = std::make_shared<Response>();
retry:
    const m_sylar::IOState recv_rt = co_await co_recvResponse(*resp, request_id);        // 接收消息
    if (recv_rt != m_sylar::IOState::SUCCESS) {
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] recv 失败, peer " << node_id
                                    << " io=" << ioStateName(recv_rt);
        co_return recv_rt;
    }

    // repair: resp_bytes 是出参，传 nullptr 会让调用方在 *resp_bytes 处崩
    if (resp_bytes != nullptr) {
        *resp_bytes = resp->data();
    }
    co_return m_sylar::IOState::SUCCESS;
}


m_sylar::Task<m_sylar::IOState> RPCSession::co_sendMessage(const Frame& frame) {
    // 数据准备
    const std::string buffer = frame.toWire();
    const size_t total_size = buffer.size();
    size_t offset = 0;

    // 加发送锁并发送
    m_sylar::CoUniqueLock lock(m_send_mutex);
    co_await lock.lock();
    while (offset < total_size)
    {
        const int send_size = co_await sendMessage(buffer.c_str() + offset, total_size - offset);
        if (send_size > 0) {
            offset += static_cast<size_t>(send_size);
            continue;
        }

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





m_sylar::Task<m_sylar::IOState> RPCSession::co_recvResponse(Response& frame, int request_id) {
    // 从缓存 检查接受缓冲区和帧缓冲区
    m_sylar::IOState st = co_await recvResponseFromBuffer(frame, request_id);
    if (st != m_sylar::IOState::FAILED) {       // 如果不是正常的查询不到（成功或错误），直接返回
        co_return st;
    }


    // 从socket 接受数据
    m_sylar::CoUniqueLock recv_lock(m_recv_mutex);
    co_await recv_lock.lock();

    // 重新查缓存
    st = co_await recvResponseFromBuffer(frame, request_id);
    if (st != m_sylar::IOState::FAILED) {
        co_return st;
    }

    size_t max_request_size = kMaxPayloadSize();
    char* buffer = nullptr;     // 输出参数，指向接收数据的起始位置，不要对buffer进行delete操作
    size_t total_length = 0;      // 累计接收字节数
    while(true) {
        // 接受缓冲区信息
        const int recv_len = co_await recvMessage(&buffer, -1);
        // 接收错误处理
        if(recv_len == 0)
        {

            M_SYLAR_LOG_WARN(g_logger) << "[rpc] peer " << node_id << " 关闭了连接 (recv=0/EOF)";
            co_return m_sylar::IOState::CLOSED;
        }
        else if(recv_len < 0)
        {
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

        // 接收信息处理,此处需要动buffer,加锁
        {
            m_sylar::CoUniqueLock buffer_lock(m_resp_buffer_mutex);
            co_await buffer_lock.lock();

            size_t parsed = 0;
            try{
                parsed = m_parser->parse(buffer, recv_len);
            } catch (std::exception &e) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser threw: " << e.what() << ", peer " << node_id;
                co_return m_sylar::IOState::CLOSED;
            }
            consume(parsed);        // 消耗buffer中的部分内容
            total_length += parsed;


            // 检查解析器状态
            if (m_parser->hasError()) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser in BADFRAME state, peer " << node_id
                                            << " 流已错位";
                co_return m_sylar::IOState::CLOSED;
            }
            buffer_lock.unlock();
            if (parsed == 0 && recv_len > 0) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser made no progress, recv_len=" << recv_len
                                            << ", peer " << node_id << " 流已错位";
                co_return m_sylar::IOState::CLOSED;
            }
        }

        // 解析状态处理
        if(total_length > max_request_size)
        {
            M_SYLAR_LOG_WARN(g_logger) << "[rpc] message is too long, total=" << total_length
                                       << " max=" << max_request_size << ", peer " << node_id
                                       << " 长度头不可信";
            co_return m_sylar::IOState::CLOSED;
        }

        // 接收帧
        st = co_await recvResponseFromBuffer(frame, request_id);
        if (st != m_sylar::IOState::FAILED) {       // 如果不是正常的查询不到（成功或错误），直接返回
            co_return st;
        }
    }

    // 此路径本不应抵达
    M_SYLAR_LOG_WARN(g_logger) << "程序进入不应到达路径";
    co_return m_sylar::IOState::SUCCESS;
}



m_sylar::Task<m_sylar::IOState> RPCSession::co_recvRequest(Request& frame) {
    // 从缓存 检查接受缓冲区和帧缓冲区
    m_sylar::IOState st = co_await recvRequestFromBuffer(frame);
    if (st != m_sylar::IOState::FAILED) {       // 如果不是正常的查询不到（成功或错误），直接返回
        co_return st;
    }


    // 从socket 接受数据
    m_sylar::CoUniqueLock recv_lock(m_recv_mutex);
    co_await recv_lock.lock();
    // 重新查缓存
    st = co_await recvRequestFromBuffer(frame);
    if (st != m_sylar::IOState::FAILED) {
        co_return st;
    }

    size_t max_request_size = kMaxPayloadSize();
    char* buffer = nullptr;     // 输出参数，指向接收数据的起始位置，不要对buffer进行delete操作
    size_t total_length = 0;      // 累计接收字节数
    while(true) {
        // 接受缓冲区信息
        const int recv_len = co_await recvMessage(&buffer, -1);
        // 接收错误处理
        if(recv_len == 0)
        {

            M_SYLAR_LOG_WARN(g_logger) << "[rpc] peer " << node_id << " 关闭了连接 (recv=0/EOF)";
            co_return m_sylar::IOState::CLOSED;
        }
        else if(recv_len < 0)
        {
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

        // 接收信息处理,此处需要动buffer,加锁
        {
            m_sylar::CoUniqueLock buffer_lock(m_req_buffer_mutex);
            co_await buffer_lock.lock();

            size_t parsed = 0;
            try{
                parsed = m_parser->parse(buffer, recv_len);
            } catch (std::exception &e) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser threw: " << e.what() << ", peer " << node_id;
                co_return m_sylar::IOState::CLOSED;
            }
            consume(parsed);        // 消耗buffer中的部分内容
            total_length += parsed;


            // 检查解析器状态
            if (m_parser->hasError()) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser in BADFRAME state, peer " << node_id
                                            << " 流已错位";
                co_return m_sylar::IOState::CLOSED;
            }
            buffer_lock.unlock();
            if (parsed == 0 && recv_len > 0) {
                M_SYLAR_LOG_ERROR(g_logger) << "[rpc] parser made no progress, recv_len=" << recv_len
                                            << ", peer " << node_id << " 流已错位";
                co_return m_sylar::IOState::CLOSED;
            }
        }

        // 解析状态处理
        if(total_length > max_request_size)
        {
            M_SYLAR_LOG_WARN(g_logger) << "[rpc] message is too long, total=" << total_length
                                       << " max=" << max_request_size << ", peer " << node_id
                                       << " 长度头不可信";
            co_return m_sylar::IOState::CLOSED;
        }

        // 接收帧
        st = co_await recvRequestFromBuffer(frame);
        if (st != m_sylar::IOState::FAILED) {       // 如果不是正常的查询不到（成功或错误），直接返回
            co_return st;
        }
    }

    // 此路径本不应抵达
    M_SYLAR_LOG_WARN(g_logger) << "程序进入不应到达路径";
    co_return m_sylar::IOState::SUCCESS;
}


void RPCSession::setAddress(const m_sylar::IPv4Address& address) const {
    *m_address = address;
}



m_sylar::Task<m_sylar::IOState> RPCSession::recvResponseFromBuffer(Response& frame, int request_id) {
    m_sylar::CoUniqueLock lock(m_resp_buffer_mutex);
    co_await lock.lock();

    while (!m_parser->empty()) {
        std::shared_ptr<Response> new_frame = std::make_shared<Response>();
        if (!new_frame->ParseFromString(m_parser->popFrame()->payload())) {
            M_SYLAR_LOG_ERROR(g_logger) << "解析frame的response帧失败";
            co_return m_sylar::IOState::CLOSED;        // 返回closed表示连接损坏
        }
        m_recved_frames[new_frame->id()] = new_frame;
    }

    auto it = m_recved_frames.find(request_id);
    if (it != m_recved_frames.end()) {
        // 帧存在，取出并返回
        frame = std::move(*it->second);
        m_recved_frames.erase(it);
        co_return m_sylar::IOState::SUCCESS;
    }
    co_return m_sylar::IOState::FAILED;            // 获取失败
}


m_sylar::Task<m_sylar::IOState> RPCSession::recvRequestFromBuffer(Request& frame) {
    // repair: 这里原来用 m_resp_buffer_mutex，但调用它的 co_recvRequest 循环体是用
    //   m_req_buffer_mutex 护 m_parser->parse() 的 —— 同一个 parser 被两把不同的锁护着，
    //   互不排斥。现在服务端一条连接只有一个读者，所以还没炸；一旦多一个读者就是
    //   parser / Session buffer 的并发损坏，而且极难查。两处统一成 m_req_buffer_mutex。
    m_sylar::CoUniqueLock lock(m_req_buffer_mutex);
    co_await lock.lock();
    // 取出解析器中一个帧
    while (!m_parser->empty()) {
        auto new_frame = std::make_shared<Request>();
        if (!new_frame->ParseFromString(m_parser->popFrame()->payload())) {
            M_SYLAR_LOG_ERROR(g_logger) << "解析frame的request帧失败";
            co_return m_sylar::IOState::CLOSED;        // 返回closed表示连接损坏
        }
        frame = std::move(*new_frame);
        co_return m_sylar::IOState::SUCCESS;
    }
    co_return m_sylar::IOState::FAILED;            // 获取失败
}
}
}