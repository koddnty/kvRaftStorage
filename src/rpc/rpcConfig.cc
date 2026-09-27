#include "rpcConfig.hpp"

#include <memory>

#include <sylar/basic/log.h>

namespace craft {
namespace RPC {

static auto g_logger = M_SYLAR_LOG_NAME("craft");

m_sylar::IPAddress::ptr toAddress(const NodeDefine& node) {
    // 注意：底层是 Address::Lookup -> getaddrinfo，而且 hints.ai_flags = 0（没设 AI_NUMERICHOST），
    // 所以这里既接受 "127.0.0.1" 也接受主机名，但主机名会走一次 DNS/NSS 解析、
    // 首包可能阻塞。只在 init 阶段每个节点调一次，别放进热路径。
    auto ip = std::dynamic_pointer_cast<m_sylar::IPv4Address>(
        m_sylar::Address::LookupAnyIPAddress(node.ip));
    if (ip) {
        ip->setPort(node.port);
    }
    return ip;
}


int loadRpcConfig(const std::string& confPath, int configId, RpcDefine& out) {
    // 1) 先把整个文件读进 ConfigData[configId]
    if (!confPath.empty()) {
        if (m_sylar::ConfigManager::LoadJson(confPath, configId) != 0) {
            out = RpcDefine{};
            out.errmsg = "配置文件打不开或者 json 解析失败: " + confPath;
            M_SYLAR_LOG_ERROR(g_logger) << "[rpc] " << out.errmsg;
            return -1;
        }
    }

    // 2) 注册配置项 —— 它内部会取出 kRpcNodesPath 指向的子 json 交给
    //    FormatConversion<nlohmann::json, RpcDefine> 反序列化。
    //    ★ 这里刻意用【局部变量】而不是全局/函数内 static：
    //      ConfigVar 在构造那一刻就把 json 抓下来了（config.h:174-187），
    //      static 一旦在 LoadJson 之前构造出来，读到的永远是默认值。
    //      局部变量在析构时会摘掉自己的监听，所以反复 init 也不会累积监听器。
    //      要热更新的话得让这个 ConfigVar 活得比 init 长，并把 LookUp 的 cb 传成"应用配置"的
    //      函数（日志模块的 updataLogger 就是这个用法）—— 现在不需要，先不做。
    auto conf = m_sylar::ConfigManager::LookUp<RpcDefine>(
        kRpcNodesPath, RpcDefine{}, configId, "rpc 节点列表");

    out = conf->getValue();
    if (!out.isValid()) {
        if (out.errmsg.empty()) {
            out.errmsg = std::string("配置里没有节点（") + confPath + " 的 " + kRpcNodesPath + "）";
        }
        M_SYLAR_LOG_ERROR(g_logger) << "[rpc] 配置解析失败: " << out.errmsg;
        return -1;
    }

    // 这里刻意【不】顺带校验 ip 合法性：toAddress 走的是 getaddrinfo（见它的注释），
    // 主机名会阻塞一次 DNS。init() 里本来就会对每个非自己的节点调一次 toAddress，
    // 在这里再查一遍等于白跑两遍；ip 非法由调用方在 toAddress 返回 nullptr 时报。
    return 0;
}

}  // namespace RPC
}  // namespace craft
