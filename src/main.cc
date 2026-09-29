//
// Created by koddnty on 2026/9/21.
//
#include <cstdio>
#include <cstdlib>
#include <string>
#include <signal.h>
#include <sylar/basic/until.h>          // m_sylar::getExecutableDir()

#include "rpc/rpcClient.hpp"
#include "rpc/rpcManager.hpp"
#include "tools/argParser.hpp"
#include "rpc/rpcConfig.hpp"

using namespace craft;
inline constexpr int kRpcConfId = 2;
static auto g_logger = M_SYLAR_LOG_NAME("craft");

m_sylar::Task<void> testTestRoute(std::shared_ptr<RPC::Request> req, RPC::RPCSession::ptr session) {
    M_SYLAR_LOG_INFO(g_logger) << "server recv from [" << req->id() << "]: " << req->data();
    RPC::Frame frame;
    RPC::Response resp;
    resp.set_id(req->id());
    resp.set_code(static_cast<int32_t>(RPC::RpcCode::OK));
    resp.set_data("hello client!");
    frame.setData(resp.SerializeAsString());
    co_await session->co_sendMessage(frame);
    co_return;
}


m_sylar::Task<void, m_sylar::TaskBeginExecuter> initRPC(RPC::RpcDefine define, int node_id) {
    // 初始化manager
    auto* mgr = RPCManager::GetInstanceWithOutInit();
    mgr->create();

    // 初始化client,server
    const int peers = co_await mgr->getClient()->init(define, node_id);
    int rt = co_await mgr->getServer()->init(define, node_id);
    if (rt == -1) {
        M_SYLAR_LOG_ERROR(g_logger) << " failed to init server [or client]";
    }

    // 路由注册
    RPCManager::GetInstanceWithOutInit()->getServer()->registeRoute("test", "test", testTestRoute);

}

volatile sig_atomic_t keep_running = 1;

void handle_signal(int sig) {
    printf("收到信号 %d，准备退出\n", sig);
    keep_running = 0;
}



m_sylar::Task<void, m_sylar::TaskBeginExecuter> circle_task(int node_id) {
    co_await m_sylar::co_sleep(5);
    m_sylar::TimeTask::ptr task = m_sylar::TimeTask::create(5000, true,
        [node_id](m_sylar::TimeTask::ptr task) -> m_sylar::Task<void> {
            std::shared_ptr<std::string> message = std::make_shared<std::string>("hello server!");
            std::shared_ptr<std::string> buffer = std::make_shared<std::string>();
            RPC::CallState st = co_await RPCManager::GetInstanceWithOutInit()->getClient()->
                call(node_id, "test", "test", message, buffer);
            M_SYLAR_LOG_INFO(g_logger) << "sending message to [" << node_id << "]";
            if (RPC::CallState::SUCCESS == st) {
                M_SYLAR_LOG_INFO(g_logger) << "recv: " << *buffer;
            }
            co_return;
        }
    );

    m_sylar::TimeManager::getInstance()->addTimer(task);
    co_return;
}




int main(int argc, char** argv) {
    // 注册信号处理函数
    signal(SIGINT, handle_signal);   // Ctrl+C
    signal(SIGTERM, handle_signal);  // kill 默认信号

    // 配置加载
    int nodeId = -1;

    std::string confPath = m_sylar::getExecutableDir() + "/conf/conf.json";
    m_sylar::ConfigManager::LoadJson(confPath, 0);

    confPath = m_sylar::getExecutableDir() + "/conf/rpc.json";

    ArgParser parser("kvRaft");
    parser.addOption("-i", "--node-id", "本节点 id（对应 conf 里 peers 的 id）",
                     [&nodeId](const std::string& v) {
                         // repair: 这里【不能】用 std::atoi —— atoi("abc") 会静默返回 0，
                         //   而 0 是合法节点号，于是 -i 打错一个字符就变成"我是 node 0"，
                         //   既不报错也看不出异常。用 strtol 并要求整串都被吃掉。
                         char* end = nullptr;
                         const long id = std::strtol(v.c_str(), &end, 10);
                         if (end == v.c_str() || *end != '\0' || id < 0 || id > 1024) {
                             return false;
                         }
                         nodeId = static_cast<int>(id);
                         return true;
                     });
    parser.addOption("-c", "--conf", "配置文件路径（默认 <仓库根>/conf/rpc.json）",
                     [&confPath](const std::string& v) {
                         confPath = v;
                         return !v.empty();
                     });

    switch (parser.parse(argc, argv)) {
        case ArgParser::Result::Help:
            std::printf("%s", parser.usage().c_str());
            return 0;
        case ArgParser::Result::Error:
            std::printf("%s", parser.usage().c_str());
            return 2;
        case ArgParser::Result::OK:
            break;
    }

    if (nodeId < 0) {
        std::printf("必须用 -i <id> 指定本节点号\n%s", parser.usage().c_str());
        return 2;
    }

    std::printf("nodeId=%d\nconfPath=%s\n", nodeId, confPath.c_str());


    // 服务初始化
    RPC::RpcDefine define;
    RPC::loadRpcConfig(confPath, kRpcConfId, define);

    auto* iom = new m_sylar::IOManager("kvraft", 1);
    iom->schedule(m_sylar::TaskCoro20::create_coro(std::bind(&initRPC, define, nodeId)));
    if (nodeId != 0) {
        iom->schedule(m_sylar::TaskCoro20::create_coro(std::bind(&circle_task, 0)));
    }


    // 注册路由
    while (keep_running) {
        pause();  // 挂起，直到收到信号
    }

    iom->autoStop();


    std::_Exit(0);
}
