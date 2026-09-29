#include "rpcServer.hpp"

namespace craft {
namespace RPC {
static auto g_logger = M_SYLAR_LOG_NAME("craft");


// RPC server ----------------------------------------------------
m_sylar::Task<int> RPCServer::init(RpcDefine define, int node_id) {
    const NodeDefine* def = define.find(node_id);
    if (def == nullptr) {
        M_SYLAR_LOG_ERROR(g_logger) << "node id " << node_id << " does not exist in rpc defins";
        co_return -1;
    }
    else {
        // bind并初始化
        m_node_id = node_id;
        uint16_t port = def->port;
        m_sylar::Address::ptr addr = m_sylar::Address::LookupAnyIPAddress(def->ip);
        std::dynamic_pointer_cast<m_sylar::IPv4Address>(addr)->setPort(port);
        bind(addr);
        start();        // 启动
    }
    co_return 0;
}


bool RPCServer::start() {
    // repair: 原来是 `m_iomanager = IOManager::getInstance();` + 遍历派生的 m_sockets ——
    //   那个 m_sockets 是空的（bind() 填的是基类那个，见 rpcServer.hpp 里的说明），
    //   所以这里改成基类的 getSockets()；m_iomanager 也统一走 getIomanager()。
    if(!isStop())
    {
        return true;
    }
    m_stop = false;

    for(auto& sock : getSockets())
    {
        auto t = std::bind(&RPCServer::startAccept, std::dynamic_pointer_cast<RPCServer>(shared_from_this()), sock);
        getIomanager()->schedule(m_sylar::TaskCoro20::create_coro(t));
    }
    return true;
}


void RPCServer::registeRoute(const std::string& service, const std::string& method, HandlerFunc func) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_route[service][method] = {func};
}




m_sylar::Task<void, m_sylar::TaskBeginExecuter> RPCServer::startAccept(m_sylar::Socket::ptr sock) {
    int count = 1000;
    while(!isStop() && --count)
    {
        m_sylar::Socket::ptr client = co_await sock->accept();       // 新连接到来时子任务resume后恢复，但子任务无法析构。
        if(client)
        {
            // std::cout << "new client" << std::endl;
            client->setRecvTimeOut(kServerRecvTimeout());
            auto t = std::bind(&RPCServer::handleClient, std::dynamic_pointer_cast<RPCServer>(shared_from_this()), client);
            getIomanager()->schedule(m_sylar::TaskCoro20::create_coro(t));       // 为新连接注册任务
        }
        else {
            if(errno == ETIMEDOUT)
            {   // 监听fd等待超时, 无新连接, 循环重新accept
            }
            else
            {
                M_SYLAR_LOG_WARN(g_logger) << "accept failed, errno : " << errno << " error : " << strerror(errno);
                co_return;
            }
        }
    }
    if (!m_stop) {
        auto t = std::bind(&RPCServer::startAccept, std::dynamic_pointer_cast<RPCServer>(shared_from_this()), sock);
        getIomanager()->schedule(m_sylar::TaskCoro20::create_coro(t));
    }
    co_return;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> RPCServer::handleClient(m_sylar::Socket::ptr client) {
    int code = 1000;
    int loopCount = 0;

    RPCSession::ptr session = std::make_shared<RPCSession>(client);
    session->setNodeId(m_node_id);
    session->setAddress(dynamic_cast<const m_sylar::IPv4Address&>(*client->getRemoteAddress()));
    session->setRecvTimeOut(kServerRecvTimeout());

    // 通信
    bool session_stopped {false};
    while (loopCount < 1000 && !m_stop) {
        loopCount++;
        // 获取请求
        std::shared_ptr<Request> req = std::make_shared<Request>();
        m_sylar::IOState state = co_await session->co_recvRequest(*req);
        if (state == m_sylar::IOState::TIMEOUT) {
            continue;
        }
        else if (state == m_sylar::IOState::CLOSED) {        // 连接关闭或超时
            co_return;
        }
        else if (state != m_sylar::IOState::SUCCESS) {
            M_SYLAR_LOG_WARN(g_logger) << "接收数据出现问题：" << ioStateName(state);
            co_return;
        }

        // 请求路由
        // repair: 原来用 m_iomanager（派生类影子成员），这里和别处统一成 getIomanager()
        getIomanager()->schedule(m_sylar::TaskCoro20::create_coro(
            std::bind(&RPCServer::coRoute, std::dynamic_pointer_cast<RPCServer>(shared_from_this()), req, session)));
    }

    // 此路径仅在需要重新调度时才会到达，异常均在while中处理了
    if (!m_stop) {
        auto t = std::bind(&RPCServer::handleClient, std::dynamic_pointer_cast<RPCServer>(shared_from_this()), client);
        getIomanager()->schedule(m_sylar::TaskCoro20::create_coro(t));
    }
    co_return;
}


// 统一给客户端回一个错误码。
static m_sylar::Task<void> replyError(RPCSession::ptr s, int id, RpcCode code, const std::string& errmsg) {
    Response resp;
    resp.set_id(id);                    // ★ id 必须原样回，客户端就是靠它做请求/响应路由
    resp.set_code(static_cast<int>(code));
    resp.set_errmsg(errmsg);
    Frame f;
    f.setData(resp.SerializeAsString());
    co_await s->co_sendMessage(f);
    co_return;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> RPCServer::coRoute(std::shared_ptr<Request> req, RPCSession::ptr session) {
    auto service = m_route.find(req->service());
    if (service != m_route.end()) {
        auto method = service->second.find(req->method());
        if (method != service->second.end()) {
            // 正常服务路径
            co_await method->second(req, session);   // 运行
            co_return;
        }
        else {
            M_SYLAR_LOG_WARN(g_logger) << "can not find rpc method " << req->service()
                                       << "." << req->method();
            co_await replyError(session, req->id(), RpcCode::UNK_METHOD,
                                "no such method: " + req->method());
            co_return;
        }
    }
    else {
        M_SYLAR_LOG_WARN(g_logger) << "can not find rpc service " << req->service();
        co_await replyError(session, req->id(), RpcCode::UNK_SERVICE,
                            "no such service: " + req->service());
    }
    co_return;
}
}
}