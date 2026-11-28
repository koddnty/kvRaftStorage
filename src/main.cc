//
// Created by koddnty on 2026/9/21.
//
#include <cstdio>
#include <cstdlib>
#include <string>
#include <signal.h>
#include <sylar/basic/until.h>          // m_sylar::getExecutableDir()
#include <sylar/server/http/httpServer.hpp>

#include "rpc/rpcClient.hpp"
#include "rpc/rpcManager.hpp"
#include "tools/argParser.hpp"
#include "rpc/rpcConfig.hpp"
#include "definations.hpp"
#include  "raft/node.hpp"
using namespace craft;
static auto g_logger = M_SYLAR_LOG_NAME("craft");


volatile sig_atomic_t keep_running = 1;
static std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>();       // 当前raft节点




void handle_signal(int sig) {
    printf("收到信号 %d，准备退出\n", sig);
    keep_running = 0;
}


// 节点初始化函数 ----------------------------------------
m_sylar::Task<void, m_sylar::TaskBeginExecuter> initRaftNode
        (std::shared_ptr<RaftNode> node, RPC::RpcDefine def, int node_id) {
    co_await node->init(def, node_id, "/home/koddnty/user/projects/KVraft/logs/");
    node->start();


    M_SYLAR_LOG_INFO(g_logger) << "初始化当前节点{" << node_id << "}" << " 完成, 节点已启动";
    co_return;
}


m_sylar::Task<void> push() {
    if (RaftNode::RespCode::SUCCESS !=  co_await node->push("hhh")) {
        M_SYLAR_LOG_INFO(g_logger) << " push log failed";
        co_return;
    }
    M_SYLAR_LOG_INFO(g_logger) << " push log successed";
    co_return;
}


int main(int argc, char** argv) {
    // 注册信号处理函数
    signal(SIGINT, handle_signal);   // Ctrl+C
    signal(SIGTERM, handle_signal);  // kill 默认信号

    // 配置加载
    int node_id = -1;

    std::string confPath = m_sylar::getExecutableDir() + "/conf/conf.json";
    m_sylar::ConfigManager::LoadJson(confPath, 0);

    confPath = m_sylar::getExecutableDir() + "/conf/rpc.json";

    ArgParser parser("kvRaft");
    parser.addOption("-i", "--node-id", "本节点 id（对应 conf 里 peers 的 id）",
                     [&node_id](const std::string& v) {
                         char* end = nullptr;
                         const long id = std::strtol(v.c_str(), &end, 10);
                         if (end == v.c_str() || *end != '\0' || id < 0 || id > 1024) {
                             return false;
                         }
                         node_id = static_cast<int>(id);
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

    if (node_id < 0) {
        std::printf("必须用 -i <id> 指定本节点号\n%s", parser.usage().c_str());
        return 2;
    }

    std::printf("nodeId=%d\nconfPath=%s\n", node_id, confPath.c_str());


    RPC::RpcDefine define;
    RPC::loadRpcConfig(confPath, kRpcConfId, define);




    // 节点初始化
    auto* iom = new m_sylar::IOManager("kvraft", 4);

    iom->schedule(m_sylar::TaskCoro20::create_coro(
        std::bind(&initRaftNode, node, define, node_id)));


    auto http_server = m_sylar::http::HttpServer(iom);

    uint16_t port = 8666 + node_id;
    m_sylar::http::HttpServer::ptr server(new m_sylar::http::HttpServer(iom));
    m_sylar::Address::ptr addr = m_sylar::Address::LookupAnyIPAddress("0.0.0.0");
    std::dynamic_pointer_cast<m_sylar::IPv4Address>(addr)->setPort(port);
    server->bind(addr, 6);

    server->GET("/push", m_sylar::http::HttpServer::HandlerFunc(push));

    server->start();
    M_SYLAR_LOG_INFO(g_logger) << "All Gate have been registered, ip:0.0.0.0:" << port;



    // 注册路由
    while (keep_running) {
        pause();  // 挂起，直到收到信号
    }

    server->stop();
    iom->autoStop();


    std::_Exit(0);
}
