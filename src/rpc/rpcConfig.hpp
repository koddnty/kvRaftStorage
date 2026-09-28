#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <sylar/basic/address.h>
#include <sylar/basic/config.h>

/**
 * @brief rpc 模块的配置文件反序列化
 *
 * 完全照 m_sylar 日志模块的做法来（basic/log.cc:593 的
 * FormatConversion<nlohmann::json, std::set<LogDefine>>）：
 *
 *   1. ConfigManager::LoadJson(path, config_id) 把整个文件读进 ConfigData[config_id]
 *   2. ConfigManager::LookUp<ValueType>(json_path, default_value, config_id, ...) 注册配置项，
 *      它内部用 getJsonValueByPath 取出 json_path 指向的那块子 json，再交给
 *      FormatConversion<nlohmann::json, ValueType>() 反序列化（basic/config.h:178-187）
 *
 * ★ 两个这个机制自带的坑，必须按这里的方式来：
 *
 *   坑 1：ConfigVar 是在【构造的那一刻】就把 json 抓下来的（config.h:174-187 直接读
 *        ConfigManager::getConfigData(...)->getJsonData()），并且顺手给自己挂上监听器。
 *        所以【不能】写成 namespace 作用域的 static/global 再 getValue() 快照 ——
 *        global 的构造发生在 main 之前，那会儿还没 LoadJson，快照到的是默认值；
 *        更糟的是 `LookUp(...)` 返回的是个临时 shared_ptr，语句一结束 ConfigVar 就被析构、
 *        监听器一起摘掉，于是配置文件里写什么都【被静默忽略】。
 *        正确做法见下面 kRpc* 那一段：ConfigVar 留成活到进程结束的 inline 全局，
 *        取值统一走访问函数，构造时拿默认值、LoadJson 时被监听器刷成文件值。
 *        注意：必须 loadRpcConfig()（= LoadJson）之后才取，别在 static 初始化里用。
 *
 *   坑 2：ConfigData::setConfig 是【整体覆盖】不是合并
 *        （config.cc:42 `m_json_data = std::move(json_data)`），
 *        所以一个 config_id 同时只能装一个文件。rpc 的全部配置（节点列表 + 各可调项）
 *        都集中在 conf/rpc.json，用 id 2（原来的 conf/craft.json 已经并进来了）。
 */
namespace craft {
namespace RPC {

/// conf/rpc.json —— rpc 模块的全部配置都在这一个文件里
inline constexpr const char* kRpcConfPath = "conf/rpc.json";
/// 该文件使用的 config_id（★ 一个 id 只能装一个文件，见上面坑 2）
inline constexpr int kRpcConfId = 2;
/// 节点列表在文件里的路径（getJsonValueByPath 用点分隔；支持 "a.b.0.c" 这种数组下标）
inline constexpr const char* kRpcNodesPath = "peers";

/// 单个节点的定义
struct NodeDefine {
    int id{-1};
    std::string ip{"127.0.0.1"};
    uint16_t port{0};

    std::string addr() const { return ip + ":" + std::to_string(port); }
};

/**
 * @brief rpc 配置
 *
 *   nodes 是【集群的全部节点，包含自己】—— 自己是哪一个由启动参数 selfId 决定，
 *   这样三个节点可以共用同一份配置文件（各自只把自己摘出去建连、自己那份用来 bind）。
 */
struct RpcDefine {
    std::vector<NodeDefine> nodes;
    std::string errmsg;         ///< 解析/校验失败的原因，空串表示成功

    bool isValid() const { return errmsg.empty() && !nodes.empty(); }

    const NodeDefine* find(int id) const {
        for (const auto& n : nodes) {
            if (n.id == id) {
                return &n;
            }
        }
        return nullptr;
    }
};

/// NodeDefine -> m_sylar 地址对象（ip 非法时返回 nullptr）
m_sylar::IPAddress::ptr toAddress(const NodeDefine& node);

// ---------------------------------------------------------------------------
// rpc 模块的全部可调项 —— 都从 conf/rpc.json 读，配置文件里的 key 和这里的变量名一一对应。
//
// ★ 为什么是"ConfigVar 全局 + 访问函数"，而不是
//       static int kRpcTimeoutUs = ConfigManager::LookUp("kRpcTimeoutUs", 100000, 2)->getValue();
//   ConfigVar 在构造时读一次 json，并给自己挂一个监听器（config.h:174-207）。
//   而 namespace 作用域的 static 是在 main 之前构造的：那时 LoadJson 还没跑，
//   读到的只能是默认值；而且 `LookUp(...)` 返回的是个【临时】shared_ptr，
//   整个语句一结束 ConfigVar 就被析构、监听器也一起摘掉 —— 快照就此冻结。
//   结果：配置文件里写什么都无效，而且不报错、不打日志，纯靠肉眼看出来。
//   所以这里把 ConfigVar 本人留在 inline 全局里（活到进程结束，监听器一直挂着），
//   取值统一走下面那几个函数：
//       静态初始化阶段构造 → 拿默认值
//       loadRpcConfig() 里 LoadJson → 监听器把文件里的值刷进 m_val
//       之后 rpcTimeoutUs() 等拿到的就是文件值 ✓
//   白捡一个好处：再 LoadJson 一次就热更新，不用重启。
//
//   调用时机：必须在 loadRpcConfig()（也就是 LoadJson）之后取，别在 static 初始化里取。
//   正常路径 RPCClient::init() 里是先 loadRpcConfig() 再建连，所以没问题；
//   绕过 init() 直接裸用 Frame/Parser/RPCSession 的地方（单元测试）拿到的是默认值。
// ---------------------------------------------------------------------------
inline m_sylar::ConfigVar<size_t>::ptr g_kLengthFieldSize =
    m_sylar::ConfigManager::LookUp<size_t>("kLengthFieldSize", 4, kRpcConfId,
                                           "帧长度头字节数（★ 线上协议，两端必须一致）");

inline m_sylar::ConfigVar<size_t>::ptr g_kMaxPayloadSize =
    m_sylar::ConfigManager::LookUp<size_t>("kMaxPayloadSize", 67108864, kRpcConfId,
                                           "单帧载荷上限(字节)，超了按不可信长度头处理");

inline m_sylar::ConfigVar<int64_t>::ptr g_kRpcTimeoutUs =
    m_sylar::ConfigManager::LookUp<int64_t>("kRpcTimeoutUs", int64_t(100 * 1000), kRpcConfId,
                                            "rpc 收发超时(us)。要远小于选举超时");

inline m_sylar::ConfigVar<int>::ptr g_kTimeoutToDisconnectCount =
    m_sylar::ConfigManager::LookUp<int>("kTimeoutToDisconnectCount", 3, kRpcConfId,
                                        "同一节点连续超时多少次按断开处理（成功即归零）");

inline m_sylar::ConfigVar<int>::ptr g_kReconnectMaxAttempts =
    m_sylar::ConfigManager::LookUp<int>("kReconnectMaxAttempts", 3, kRpcConfId,
                                        "单轮重连的尝试上限，用完就放弃、等 coConnectAll 下一轮");

inline m_sylar::ConfigVar<unsigned int>::ptr g_kReconnectBackoffMs =
    m_sylar::ConfigManager::LookUp<unsigned int>("kReconnectBackoffMs", 5, kRpcConfId,
                                                 "重连首次退避(ms)，之后指数增长");

inline m_sylar::ConfigVar<unsigned int>::ptr g_kReconnectMaxBackoffMs =
    m_sylar::ConfigManager::LookUp<unsigned int>("kReconnectMaxBackoffMs", 20, kRpcConfId,
                                                 "重连退避上限(ms)");

inline m_sylar::ConfigVar<int64_t>::ptr g_kServerRecvTimeout =
    m_sylar::ConfigManager::LookUp<int64_t>("kServerRecvTimeout", 30000000, kRpcConfId,
                                                 "服务端接收超us");
/// 取值统一走这几个函数（原因见上面那段说明）
inline size_t       kLengthFieldSize()              { return g_kLengthFieldSize->getValue(); }
inline size_t       kMaxPayloadSize()               { return g_kMaxPayloadSize->getValue(); }
inline int64_t      kRpcTimeoutUs()                 { return g_kRpcTimeoutUs->getValue(); }
inline int          kTimeoutToDisconnectCount()     { return g_kTimeoutToDisconnectCount->getValue(); }
inline unsigned int kReconnectBackoffMs()           { return g_kReconnectBackoffMs->getValue(); }
inline unsigned int kReconnectMaxBackoffMs()        { return g_kReconnectMaxBackoffMs->getValue(); }
inline int64_t      kServerRecvTimeout()            { return g_kServerRecvTimeout->getValue(); }
/// 尝试次数至少为 1：配置里写 0 或负数时别退化成"永远不重连"
inline int          kReconnectMaxAttempts() {
    const int v = g_kReconnectMaxAttempts->getValue();
    return v > 0 ? v : 1;
}

/**
 * @brief 读配置文件并反序列化成 RpcDefine
 *
 * 注意 confPath 为空时只做 LookUp（走默认值），文件不覆盖；重复调用会重新 LoadJson。
 *
 * @param confPath  配置文件路径，空串表示不读文件
 * @param configId  该文件对应的 config_id
 * @param out       出参，成功时 isValid() 为 true，失败时 errmsg 有原因
 * @return 成功返回 0，失败返回 -1
 */
int loadRpcConfig(const std::string& confPath, int configId, RpcDefine& out);

}  // namespace RPC
}  // namespace craft

// ---------------------------------------------------------------------------
// json -> NodeDefine / RpcDefine 的转换器。
//
// 写法和日志模块保持一致：手动逐字段取值，并且【每取一个字段都先 find() 判存在】。
//   config.h:62 明确提醒过：错误的 FormatConversion 实现会导致程序崩溃
//   （const json 上用 operator[] 取不存在的 key 就是 UB / 断言失败）。
// 这里选择"出错就填 errmsg 并立刻返回"，不做裸取、也不抛异常 —— 缺字段是配置问题，
// 应该由调用方打印出来，而不是把进程带走。
// ---------------------------------------------------------------------------
namespace m_sylar {

template<>
class FormatConversion<nlohmann::json, craft::RPC::NodeDefine> {
public:
    craft::RPC::NodeDefine operator()(const nlohmann::json& v) {
        craft::RPC::NodeDefine node;
        if (!v.is_object()) {
            return node;        // id 留 -1 表示非法，调用方靠 id < 0 判断
        }
        if (v.find("id") != v.end() && v["id"].is_number_integer()) {
            node.id = v["id"].get<int>();
        } else {
            return node;
        }
        if (v.find("ip") != v.end() && v["ip"].is_string()) {
            node.ip = v["ip"].get<std::string>();
        }
        if (v.find("port") != v.end() && v["port"].is_number_integer()) {
            node.port = static_cast<uint16_t>(v["port"].get<int>());
        } else {
            node.id = -1;       // 缺 port 视为非法
        }
        return node;
    }
};

template<>
class FormatConversion<nlohmann::json, craft::RPC::RpcDefine> {
public:
    craft::RPC::RpcDefine operator()(const nlohmann::json& v) {
        using craft::RPC::NodeDefine;
        using craft::RPC::RpcDefine;

        RpcDefine def;

        // 定位节点列表：整个文档就是数组（conf/rpc.json 现在的样子），
        // 或者文档里带一层 peers / nodes —— 两种都认。
        const nlohmann::json* list = nullptr;
        if (v.is_array()) {
            list = &v;
        } else if (v.is_object()) {
            if (v.find("peers") != v.end()) {
                list = &v["peers"];
            } else if (v.find("nodes") != v.end()) {
                list = &v["nodes"];
            }
        }

        if (list == nullptr) {
            def.errmsg = "找不到节点列表（期望数组，或对象里的 peers / nodes 字段）";
            return def;
        }
        if (!list->is_array() || list->empty()) {
            def.errmsg = "节点列表必须是非空数组";
            return def;
        }

        for (size_t i = 0; i < list->size(); ++i) {
            const nlohmann::json& item = (*list)[i];
            NodeDefine node = FormatConversion<nlohmann::json, NodeDefine>()(item);
            if (node.id < 0) {
                def.errmsg = "节点列表第 " + std::to_string(i) + " 项非法（需要整数 id 和 port）";
                return def;
            }
            if (node.port == 0) {
                def.errmsg = "节点 " + std::to_string(node.id) + " 的 port 为 0";
                return def;
            }
            def.nodes.push_back(std::move(node));
        }

        // 校验：id 重复 / 地址重复 —— 这两种错不早报，后面握手和选主会非常难查
        for (size_t i = 0; i < def.nodes.size(); ++i) {
            for (size_t j = i + 1; j < def.nodes.size(); ++j) {
                if (def.nodes[i].id == def.nodes[j].id) {
                    def.errmsg = "节点 id 重复: " + std::to_string(def.nodes[i].id);
                    return def;
                }
                if (def.nodes[i].ip == def.nodes[j].ip && def.nodes[i].port == def.nodes[j].port) {
                    def.errmsg = "节点地址重复: " + def.nodes[i].addr();
                    return def;
                }
            }
        }
        return def;
    }
};

}  // namespace m_sylar
